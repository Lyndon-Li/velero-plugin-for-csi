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

package backup

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"

	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
)

// PVCBackupItemAction is a backup item action plugin for Velero.
type PVCBackupItemAction struct {
	Log logrus.FieldLogger
}

// AppliesTo returns information indicating that the PVCBackupItemAction should be invoked to backup PVCs.
func (p *PVCBackupItemAction) AppliesTo() (velero.ResourceSelector, error) {
	p.Log.Debug("PVCBackupItemAction AppliesTo")

	return velero.ResourceSelector{
		IncludedResources: []string{"persistentvolumeclaims"},
	}, nil
}

// Execute recognizes PVCs backed by volumes provisioned by CSI drivers with volumesnapshotting capability and creates snapshots of the
// underlying PVs by creating volumesnapshot CSI API objects that will trigger the CSI driver to perform the snapshot operation on the volume.
func (p *PVCBackupItemAction) Execute(item runtime.Unstructured, backup *velerov1api.Backup) (runtime.Unstructured, []velero.ResourceIdentifier, error) {
	p.Log.Info("Starting PVCBackupItemAction")

	// Do nothing if volume snapshots have not been requested in this backup
	if boolptr.IsSetToFalse(backup.Spec.SnapshotVolumes) {
		p.Log.Infof("Volume snapshotting not requested for backup %s/%s", backup.Namespace, backup.Name)
		return item, nil, nil
	}

	var pvc corev1api.PersistentVolumeClaim
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.UnstructuredContent(), &pvc); err != nil {
		return nil, nil, errors.WithStack(err)
	}

	client, snapshotClient, err := util.GetClients()
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	p.Log.Debugf("Fetching underlying PV for PVC %s", fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name))
	// Do nothing if this is not a CSI provisioned volume
	pv, err := util.GetPVForPVC(&pvc, client.CoreV1())
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	if pv.Spec.PersistentVolumeSource.CSI == nil {
		p.Log.Infof("Skipping PVC %s/%s, associated PV %s is not a CSI volume", pvc.Namespace, pvc.Name, pv.Name)
		return item, nil, nil
	}

	// Do nothing if FS uploader is used to backup this PV
	isFSUploaderUsed, err := util.IsPVCDefaultToFSBackup(pvc.Namespace, pvc.Name, client.CoreV1(), boolptr.IsSetToTrue(backup.Spec.DefaultVolumesToFsBackup))
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	if isFSUploaderUsed {
		p.Log.Infof("Skipping  PVC %s/%s, PV %s will be backed up using FS uploader", pvc.Namespace, pvc.Name, pv.Name)
		return item, nil, nil
	}

	// no storage class: we don't know how to map to a VolumeSnapshotClass
	if pvc.Spec.StorageClassName == nil {
		return item, nil, errors.Errorf("Cannot snapshot PVC %s/%s, PVC has no storage class.", pvc.Namespace, pvc.Name)
	}

	p.Log.Infof("Fetching storage class for PV %s", *pvc.Spec.StorageClassName)
	storageClass, err := client.StorageV1().StorageClasses().Get(context.Background(), *pvc.Spec.StorageClassName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, errors.Wrap(err, "error getting storage class")
	}
	p.Log.Debugf("Fetching volumesnapshot class for %s", storageClass.Provisioner)
	snapshotClass, err := util.GetVolumeSnapshotClassForStorageClass(storageClass.Provisioner, snapshotClient.SnapshotV1())
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to get volumesnapshotclass for storageclass %s", storageClass.Name)
	}
	p.Log.Infof("volumesnapshot class=%s", snapshotClass.Name)

	// Craft the snapshot object to be created
	snapshot := snapshotv1api.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "velero-" + pvc.Name + "-",
			Namespace:    pvc.Namespace,
			Labels: map[string]string{
				velerov1api.BackupNameLabel: label.GetValidName(backup.Name),
			},
		},
		Spec: snapshotv1api.VolumeSnapshotSpec{
			Source: snapshotv1api.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvc.Name,
			},
			VolumeSnapshotClassName: &snapshotClass.Name,
		},
	}

	upd, err := snapshotClient.SnapshotV1().VolumeSnapshots(pvc.Namespace).Create(context.Background(), &snapshot, metav1.CreateOptions{})
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error creating volume snapshot")
	}
	p.Log.Infof("Created volumesnapshot %s", fmt.Sprintf("%s/%s", upd.Namespace, upd.Name))

	labels := map[string]string{
		util.VolumeSnapshotLabel:    upd.Name,
		velerov1api.BackupNameLabel: backup.Name,
	}

	annotations := labels
	annotations[util.MustIncludeAdditionalItemAnnotation] = "true"

	util.AddAnnotations(&pvc.ObjectMeta, annotations)
	util.AddLabels(&pvc.ObjectMeta, labels)

	additionalItems := []velero.ResourceIdentifier{}
	if util.IsMovingVolumeSnapshot() {
		err := util.WaitVolumeSnapshotReady(context.Background(), snapshotClient, upd, util.GetVolumeSnapshotWaitTimeout(), p.Log)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "error wait volume snapshot ready")
		}

		backupContext, err := moveVolumeSnapshot(context.Background(), client, backup, upd, p.Log)
		if err != nil {
			p.Log.Errorf("Failed to submit data movement VolumeSnapshot %s/%s", fmt.Sprintf("%s/%s", upd.Namespace, upd.Name))

			util.DeleteVolumeSnapshotIfAny(context.Background(), snapshotClient, upd, p.Log)
			return nil, nil, errors.Wrapf(err, "error creating volume snapshot")
		} else {
			p.Log.Debugf("Data movement for VolumeSnapshot %s/%s is submitted successfully", fmt.Sprintf("%s/%s", upd.Namespace, upd.Name))
			util.AsyncWatchSnapshotBackup(context.Background(), client, snapshotClient, backupContext, p.Log)
		}
	} else {
		additionalItems = []velero.ResourceIdentifier{
			{
				GroupResource: kuberesource.VolumeSnapshots,
				Namespace:     upd.Namespace,
				Name:          upd.Name,
			},
		}
	}

	p.Log.Infof("Returning from PVCBackupItemAction with %d additionalItems to backup", len(additionalItems))
	for _, ai := range additionalItems {
		p.Log.Debugf("%s: %s", ai.GroupResource.String(), ai.Name)
	}

	pvcMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pvc)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	return &unstructured.Unstructured{Object: pvcMap}, additionalItems, nil
}

func moveVolumeSnapshot(ctx context.Context, kubeClient *kubernetes.Clientset, backup *velerov1api.Backup,
	volumeSnapshot *snapshotv1api.VolumeSnapshot, log logrus.FieldLogger) (*util.SnpahotBackupContext, error) {
	snapshotRef := &corev1api.TypedLocalObjectReference{
		APIGroup: &snapshotv1api.SchemeGroupVersion.Group,
		Kind:     "VolumeSnapshot",
		Name:     volumeSnapshot.Name,
	}

	pvc := &corev1api.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    backup.Namespace,
			GenerateName: backup.Name + "-",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: velerov1api.SchemeGroupVersion.String(),
					Kind:       "Backup",
					Name:       backup.Name,
					UID:        backup.UID,
					Controller: boolptr.True(),
				},
			},
			Labels: map[string]string{
				velerov1api.BackupNameLabel: label.GetValidName(backup.Name),
				velerov1api.BackupUIDLabel:  string(backup.UID),
			},
		},
		Spec: corev1api.PersistentVolumeClaimSpec{
			DataSource:    snapshotRef,
			DataSourceRef: snapshotRef,
		},
	}

	created, err := kubeClient.CoreV1().PersistentVolumeClaims(backup.Namespace).Create(ctx, pvc, metav1.CreateOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error to create pvc")
	}

	snapshotPV, err := util.GetPVForPVC(created, kubeClient.CoreV1())
	if err != nil {
		util.DeletePVCIfAny(ctx, kubeClient, created, log)
		return nil, errors.Wrap(err, "error to create pv from snapshot")
	}

	snapshotBackup, err := util.CreateSnapshotBackup(ctx, backup, volumeSnapshot, snapshotPV)
	if err != nil {
		util.DeletePVCIfAny(ctx, kubeClient, created, log)
		return nil, errors.Wrap(err, "error to create SnapshotBackup CR")
	}

	return &util.SnpahotBackupContext{
		VolumeSnapshot: volumeSnapshot,
		SnapshotPVC:    created,
		SnapshotBackup: snapshotBackup}, nil
}
