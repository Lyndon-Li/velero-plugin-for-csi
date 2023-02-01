/*
Copyright 2019, 2020 the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package restore

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1api "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"

	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	veleroClientSet "github.com/vmware-tanzu/velero/pkg/generated/clientset/versioned"
)

const (
	AnnBindCompleted          = "pv.kubernetes.io/bind-completed"
	AnnBoundByController      = "pv.kubernetes.io/bound-by-controller"
	AnnStorageProvisioner     = "volume.kubernetes.io/storage-provisioner"
	AnnBetaStorageProvisioner = "volume.beta.kubernetes.io/storage-provisioner"
	AnnSelectedNode           = "volume.kubernetes.io/selected-node"
)

// PVCRestoreItemAction is a restore item action plugin for Velero
type PVCRestoreItemAction struct {
	Log logrus.FieldLogger
}

// AppliesTo returns information indicating that the PVCRestoreItemAction should be run while restoring PVCs.
func (p *PVCRestoreItemAction) AppliesTo() (velero.ResourceSelector, error) {
	return velero.ResourceSelector{
		IncludedResources: []string{"persistentvolumeclaims"},
		//TODO: add label selector volumeSnapshotLabel
	}, nil
}

func removePVCAnnotations(pvc *corev1api.PersistentVolumeClaim, remove []string) {
	if pvc.Annotations == nil {
		pvc.Annotations = make(map[string]string)
		return
	}
	for k := range pvc.Annotations {
		if util.Contains(remove, k) {
			delete(pvc.Annotations, k)
		}
	}
}

// Execute modifies the PVC's spec to use the volumesnapshot object as the data source ensuring that the newly provisioned volume
// can be pre-populated with data from the volumesnapshot.
func (p *PVCRestoreItemAction) Execute(input *velero.RestoreItemActionExecuteInput) (*velero.RestoreItemActionExecuteOutput, error) {
	var pvc corev1api.PersistentVolumeClaim
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(input.Item.UnstructuredContent(), &pvc)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	p.Log.Infof("Starting PVCRestoreItemAction for PVC %s/%s", pvc.Namespace, pvc.Name)

	removePVCAnnotations(&pvc,
		[]string{AnnBindCompleted, AnnBoundByController, AnnStorageProvisioner, AnnBetaStorageProvisioner, AnnSelectedNode})

	// If cross-namespace restore is configured, change the namespace
	// for PVC object to be restored
	if val, ok := input.Restore.Spec.NamespaceMapping[pvc.GetNamespace()]; ok {
		pvc.SetNamespace(val)
	}

	_, snapClient, veleroClient, err := util.GetFullClients()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if boolptr.IsSetToTrue(input.Restore.Spec.CSISnapshotMoveData) {
		curLog := p.Log.WithField("target PVC", fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name))
		curLog.Info("Starting data movement restore")

		snapshotRestore, err := restoreFromDataMovement(context.Background(), veleroClient, input.Restore, &pvc, p.Log)
		if err != nil {
			curLog.WithError(err).Error("Failed to submit data movement restore")
			return nil, errors.WithStack(err)
		} else {
			curLog.WithField("snapshotRestore name", snapshotRestore.Name).Info("Data movement restore is submitted successfully")
		}
	} else {
		err = restoreFromVolumeSnapshot(&pvc, snapClient, p.Log)
		if err != nil {
			p.Log.WithError(err).Error("Failed to restore PVC %s/%s from volume snapshot", pvc.Namespace, pvc.Name)
			return nil, errors.WithStack(err)
		}
	}

	pvcMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pvc)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	p.Log.Infof("Returning from PVCRestoreItemAction for PVC %s/%s", pvc.Namespace, pvc.Name)

	return &velero.RestoreItemActionExecuteOutput{
		UpdatedItem: &unstructured.Unstructured{Object: pvcMap},
	}, nil
}

func restoreFromVolumeSnapshot(pvc *corev1api.PersistentVolumeClaim, snapClient *snapshotterClientSet.Clientset, Log logrus.FieldLogger) error {
	volumeSnapshotName, ok := pvc.Annotations[util.VolumeSnapshotLabel]
	if !ok {
		Log.Infof("Skipping PVCRestoreItemAction for PVC %s/%s, PVC does not have a CSI volumesnapshot.", pvc.Namespace, pvc.Name)
		return nil
	}

	vs, err := snapClient.SnapshotV1().VolumeSnapshots(pvc.Namespace).Get(context.TODO(), volumeSnapshotName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrapf(err, fmt.Sprintf("Failed to get Volumesnapshot %s/%s to restore PVC %s/%s", pvc.Namespace, volumeSnapshotName, pvc.Namespace, pvc.Name))
	}

	if _, exists := vs.Annotations[util.VolumeSnapshotRestoreSize]; exists {
		restoreSize, err := resource.ParseQuantity(vs.Annotations[util.VolumeSnapshotRestoreSize])
		if err != nil {
			return errors.Wrapf(err, fmt.Sprintf("Failed to parse %s from annotation on Volumesnapshot %s/%s into restore size",
				vs.Annotations[util.VolumeSnapshotRestoreSize], vs.Namespace, vs.Name))
		}
		// It is possible that the volume provider allocated a larger capacity volume than what was requested in the backed up PVC.
		// In this scenario the volumesnapshot of the PVC will endup being larger than its requested storage size.
		// Such a PVC, on restore as-is, will be stuck attempting to use a Volumesnapshot as a data source for a PVC that
		// is not large enough.
		// To counter that, here we set the storage request on the PVC to the larger of the PVC's storage request and the size of the
		// VolumeSnapshot
		util.SetPVCStorageResourceRequest(pvc, restoreSize, Log)
	}

	util.ResetPVCSpec(pvc, volumeSnapshotName)

	return nil
}

func restoreFromDataMovement(ctx context.Context, veleroClient *veleroClientSet.Clientset, restore *velerov1api.Restore, pvc *corev1api.PersistentVolumeClaim, log logrus.FieldLogger) (*velerov1api.SnapshotRestore, error) {
	snapshotBackup, err := getSnapshotBackupInfo(ctx, restore, veleroClient)
	if err != nil {
		return nil, errors.Wrapf(err, "error to get backup info for restore %s", restore.Name)
	}

	pvc.Spec.VolumeName = "snapshot-restore-" + snapshotBackup.Status.SnapshotID

	snapshotRestore := newSnapshotRestore(restore, snapshotBackup, pvc)
	snapshotRestore, err = veleroClient.VeleroV1().SnapshotRestores(snapshotRestore.Namespace).Create(ctx, snapshotRestore, metav1.CreateOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error to create SnapshotRestore CR")
	}

	log.WithField("SnapshotRestore CR", snapshotRestore.Name).Info("SnapshotRestore CR is created")

	return snapshotRestore, nil
}

func newSnapshotRestore(restore *velerov1api.Restore, snapshotBackup *velerov1api.SnapshotBackup, pvc *corev1api.PersistentVolumeClaim) *velerov1api.SnapshotRestore {
	snapshotRestore := &velerov1api.SnapshotRestore{
		TypeMeta: metav1.TypeMeta{
			APIVersion: velerov1api.SchemeGroupVersion.String(),
			Kind:       "SnapshotRestore",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    restore.Namespace,
			GenerateName: restore.Name + "-",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: velerov1api.SchemeGroupVersion.String(),
					Kind:       "Restore",
					Name:       restore.Name,
					UID:        restore.UID,
					Controller: boolptr.True(),
				},
			},
			Labels: map[string]string{
				velerov1api.RestoreNameLabel: label.GetValidName(restore.Name),
				velerov1api.RestoreUIDLabel:  string(restore.UID),
			},
		},
		Spec: velerov1api.SnapshotRestoreSpec{
			TargetVolume: velerov1api.TargetVolumeSpec{
				PVC:              pvc.Name,
				PV:               pvc.Spec.VolumeName,
				Namespace:        pvc.Namespace,
				StorageClass:     *pvc.Spec.StorageClassName,
				Resources:        pvc.Spec.Resources,
				OperationTimeout: snapshotBackup.Spec.CSISnapshot.CSISnapshotTimeout,
			},
			RestoreName:           restore.Name,
			BackupName:            snapshotBackup.Spec.BackupName,
			BackupStorageLocation: snapshotBackup.Spec.BackupStorageLocation,
			DataMover:             snapshotBackup.Spec.DataMover,
			SnapshotID:            snapshotBackup.Status.SnapshotID,
			SourceNamespace:       snapshotBackup.Spec.SourceNamespace,
		},
	}

	return snapshotRestore
}

func getSnapshotBackupInfo(ctx context.Context, restore *velerov1api.Restore,
	veleroClient *veleroClientSet.Clientset) (*velerov1api.SnapshotBackup, error) {
	snapshotBackupList, err := veleroClient.VeleroV1().SnapshotBackups(restore.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", velerov1api.BackupNameLabel, label.GetValidName(restore.Spec.BackupName)),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get snapshot backup with name %s", restore.Spec.BackupName)
	}

	if len(snapshotBackupList.Items) == 0 {
		return nil, errors.Errorf("no snapshot backup found for %s", restore.Spec.BackupName)
	}

	if len(snapshotBackupList.Items) > 1 {
		return nil, errors.Errorf("multiple snapshot backups found for %s", restore.Spec.BackupName)
	}

	return &snapshotBackupList.Items[0], nil
}
