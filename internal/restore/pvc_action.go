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
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1api "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"

	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	veleroClientSet "github.com/vmware-tanzu/velero/pkg/generated/clientset/versioned"
	"k8s.io/client-go/kubernetes"
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

type snpahotRestoreContext struct {
	targetPVC       *corev1api.PersistentVolumeClaim
	restorePVC      *corev1api.PersistentVolumeClaim
	snapshotRestore *velerov1api.SnapshotRestore
	cancelRoutine   context.CancelFunc
	completeStatus  util.DataMoveCompletionStatus
	completeMsg     string
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

	client, snapClient, veleroClient, err := util.GetFullClients()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if util.IsMovingVolumeSnapshot() {
		restoreContext, err := restoreFromDataMovement(context.Background(), client, veleroClient, input.Restore, &pvc, p.Log)
		if err != nil {
			p.Log.WithError(err).Error("Failed to restore PVC %s/%s from data movement", pvc.Namespace, pvc.Name)
			return nil, errors.WithStack(err)
		}

		asyncWatchSnapshotRestore(context.Background(), client, veleroClient, restoreContext, p.Log)
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

func restoreFromDataMovement(ctx context.Context, kubeClient *kubernetes.Clientset, veleroClient *veleroClientSet.Clientset,
	restore *velerov1api.Restore, pvc *corev1api.PersistentVolumeClaim, log logrus.FieldLogger) (*snpahotRestoreContext, error) {

	snapshotBackup, sourceNamespace, err := getSnapshotBackupInfo(ctx, restore, veleroClient)
	if err != nil {
		return nil, errors.Wrapf(err, "error to get backup info for restore %s", restore.Name)
	}

	volumeMode := util.GetVolumeModeFromRestore(restore)

	pvcObj := &corev1api.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    restore.Namespace,
			GenerateName: "snapshot-restore-" + restore.Name + "-",
		},
		Spec: corev1api.PersistentVolumeClaimSpec{
			AccessModes: []corev1api.PersistentVolumeAccessMode{
				corev1api.ReadWriteOnce,
			},
			StorageClassName: pvc.Spec.StorageClassName,
			VolumeMode:       &volumeMode,
			Resources:        pvc.Spec.Resources,
		},
	}

	restorePVC, err := kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(ctx, pvcObj, metav1.CreateOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error to create restore pvc")
	}

	log.Infof("Restore PVC %s is created", restorePVC.Name)

	snapshotRestore, err := createSnapshotRestore(ctx, veleroClient, restore, snapshotBackup, sourceNamespace, restorePVC)
	if err != nil {
		return nil, errors.Wrap(err, "error to create SnapshotBackup CR")
	}

	log.Infof("SnapshotRestore CR %s is created", snapshotRestore.Name)

	return &snpahotRestoreContext{
		targetPVC:       pvc,
		restorePVC:      restorePVC,
		snapshotRestore: snapshotRestore}, nil
}

func newSnapshotRestore(restore *velerov1api.Restore, snapshotBackup *velerov1api.SnapshotBackup,
	sourceNamespace string, targetPVC *corev1api.PersistentVolumeClaim) *velerov1api.SnapshotRestore {
	snapshotRestore := &velerov1api.SnapshotRestore{
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
			Pvc:                   targetPVC.Name,
			SnapshotID:            snapshotBackup.Status.SnapshotID,
			BackupStorageLocation: snapshotBackup.Spec.BackupStorageLocation,
			UploaderType:          util.GetUploaderType(),
			SourceNamespace:       sourceNamespace,
		},
	}

	return snapshotRestore
}

func createSnapshotRestore(ctx context.Context, veleroClient *veleroClientSet.Clientset,
	restore *velerov1api.Restore, snapshotBackup *velerov1api.SnapshotBackup,
	sourceNamespace string, targetPVC *corev1api.PersistentVolumeClaim) (*velerov1api.SnapshotRestore, error) {
	snapshotRestore := newSnapshotRestore(restore, snapshotBackup, sourceNamespace, targetPVC)
	snapshotRestore, err := veleroClient.VeleroV1().SnapshotRestores(snapshotRestore.Namespace).Create(ctx, snapshotRestore, metav1.CreateOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error to create SnapshotRestore CR")
	}

	return snapshotRestore, nil
}

func asyncWatchSnapshotRestore(ctx context.Context, kubeClient *kubernetes.Clientset, veleroClient *veleroClientSet.Clientset,
	restoreContext *snpahotRestoreContext, log logrus.FieldLogger) {
	cancelCtx, cancel := context.WithCancel(ctx)
	restoreContext.cancelRoutine = cancel
	go func() {
		watchSnapshotRestore(cancelCtx, veleroClient, restoreContext, log)
		if restoreContext.completeStatus == util.DataMoveCompleted {
			err := util.RebindPV(restoreContext.targetPVC, restoreContext.restorePVC, log)
			if err != nil {
				restoreContext.completeStatus = util.DataMoveFailed
				restoreContext.completeMsg = err.Error()
				log.WithError(err).Errorf("Failed to bind PV %s to pvc %s for snapshotRestore %s",
					restoreContext.restorePVC.Spec.VolumeName, restoreContext.targetPVC.Name, restoreContext.snapshotRestore.Name)
			}
		}

		cancel()
	}()
}

func watchSnapshotRestore(ctx context.Context, veleroClient *veleroClientSet.Clientset,
	restoreContext *snpahotRestoreContext, log logrus.FieldLogger) {

	checkFunc := func(ctx context.Context) (bool, error) {
		updated, err := veleroClient.VeleroV1().SnapshotRestores(restoreContext.snapshotRestore.Namespace).Get(ctx, restoreContext.snapshotRestore.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		if updated.Status.Phase == velerov1api.SnapshotRestorePhaseFailed {
			return false, errors.New("snapshot backup failed")
		}

		if updated.Status.Phase == velerov1api.SnapshotRestorePhaseCompleted {
			return true, nil
		}

		return false, nil
	}

	err := wait.PollWithContext(ctx, time.Millisecond*500, util.GetDataMovementWaitTimeout(), checkFunc)
	if err != nil {
		restoreContext.completeStatus = util.DataMoveFailed
		restoreContext.completeMsg = err.Error()
		log.WithError(err).Errorf("SnapshotRestore %s failed", restoreContext.snapshotRestore.Name)
	} else {
		restoreContext.completeStatus = util.DataMoveCompleted
		log.Infof("SnapshotRestore %s is completed", restoreContext.snapshotRestore.Name)
	}
}

func getSnapshotBackupInfo(ctx context.Context, restore *velerov1api.Restore,
	veleroClient *veleroClientSet.Clientset) (*velerov1api.SnapshotBackup, string, error) {
	snapshotBackupList, err := veleroClient.VeleroV1().SnapshotBackups(restore.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", velerov1api.BackupNameLabel, label.GetValidName(restore.Spec.BackupName)),
	})
	if err != nil {
		return nil, "", errors.Wrapf(err, "failed to get snapshot backup with name %s", restore.Spec.BackupName)
	}

	if len(snapshotBackupList.Items) == 0 {
		return nil, "", errors.Errorf("no snapshot backup found for %s", restore.Spec.BackupName)
	}

	if len(snapshotBackupList.Items) > 1 {
		return nil, "", errors.Errorf("multiple snapshot backups found for %s", restore.Spec.BackupName)
	}

	sourceNamespace, exist := snapshotBackupList.Items[0].Spec.Tags["volume"]
	if !exist || sourceNamespace == "" {
		return nil, "", errors.Errorf("the snapshot backup %s doesn't contain a valid source namespace", restore.Spec.BackupName)
	}

	return &snapshotBackupList.Items[0], sourceNamespace, nil
}
