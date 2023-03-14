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
	"encoding/json"
	"fmt"
	"os"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"

	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"

	veleroClientSet "github.com/vmware-tanzu/velero/pkg/generated/clientset/versioned"
	biav2 "github.com/vmware-tanzu/velero/pkg/plugin/velero/backupitemaction/v2"

	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
)

// PVCBackupItemAction is a backup item action v2 plugin for Velero.
type PVCBackupItemAction struct {
	Log            logrus.FieldLogger
	Client         *kubernetes.Clientset
	SnapshotClient *snapshotterClientSet.Clientset
	VeleroClient   *veleroClientSet.Clientset
}

// Name is required to implement the interface, but the Velero pod does not delegate this
// method -- it's used to tell velero what name it was registered under. The plugin implementation
// must define it, but it will never actually be called.
func (p *PVCBackupItemAction) Name() string {
	return "PVCBackupItemAction"
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
func (p *PVCBackupItemAction) Execute(item runtime.Unstructured, backup *velerov1api.Backup) (runtime.Unstructured, []velero.ResourceIdentifier,
	string, []velero.ResourceIdentifier, error) {
	p.Log.WithField("pid", os.Getpid()).Info("Starting PVCBackupItemAction")

	// Do nothing if volume snapshots have not been requested in this backup
	if boolptr.IsSetToFalse(backup.Spec.SnapshotVolumes) {
		p.Log.Infof("Volume snapshotting not requested for backup %s/%s", backup.Namespace, backup.Name)
		return item, nil, "", nil, nil
	}

	if backup.Status.Phase == velerov1api.BackupPhaseFinalizing ||
		backup.Status.Phase == velerov1api.BackupPhaseFinalizingPartiallyFailed {
		p.Log.WithField("Backup", fmt.Sprintf("%s/%s", backup.Namespace, backup.Name)).
			WithField("Phase", backup.Status.Phase).Info("Backup is in finalizing phase, skip it")
		return item, nil, "", nil, nil
	}

	var pvc corev1api.PersistentVolumeClaim
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.UnstructuredContent(), &pvc); err != nil {
		return nil, nil, "", nil, errors.WithStack(err)
	}

	p.Log.Debugf("Fetching underlying PV for PVC %s", fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name))
	// Do nothing if this is not a CSI provisioned volume
	pv, err := util.GetPVForPVC(&pvc, p.Client.CoreV1())
	if err != nil {
		return nil, nil, "", nil, errors.WithStack(err)
	}

	if pv.Spec.PersistentVolumeSource.CSI == nil {
		p.Log.Infof("Skipping PVC %s/%s, associated PV %s is not a CSI volume", pvc.Namespace, pvc.Name, pv.Name)
		return item, nil, "", nil, nil
	}

	// Do nothing if FS uploader is used to backup this PV
	isFSUploaderUsed, err := util.IsPVCDefaultToFSBackup(pvc.Namespace, pvc.Name, p.Client.CoreV1(), boolptr.IsSetToTrue(backup.Spec.DefaultVolumesToFsBackup))
	if err != nil {
		return nil, nil, "", nil, errors.WithStack(err)
	}
	if isFSUploaderUsed {
		p.Log.Infof("Skipping  PVC %s/%s, PV %s will be backed up using FS uploader", pvc.Namespace, pvc.Name, pv.Name)
		return item, nil, "", nil, nil
	}

	// no storage class: we don't know how to map to a VolumeSnapshotClass
	if pvc.Spec.StorageClassName == nil {
		return nil, nil, "", nil, errors.Errorf("Cannot snapshot PVC %s/%s, PVC has no storage class.", pvc.Namespace, pvc.Name)
	}

	p.Log.Infof("Fetching storage class for PV %s", *pvc.Spec.StorageClassName)
	storageClass, err := p.Client.StorageV1().StorageClasses().Get(context.Background(), *pvc.Spec.StorageClassName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, "", nil, errors.Wrap(err, "error getting storage class")
	}
	p.Log.Debugf("Fetching volumesnapshot class for %s", storageClass.Provisioner)
	snapshotClass, err := util.GetVolumeSnapshotClassForStorageClass(storageClass.Provisioner, p.SnapshotClient.SnapshotV1())
	if err != nil {
		return nil, nil, "", nil, errors.Wrapf(err, "failed to get volumesnapshotclass for storageclass %s", storageClass.Name)
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

	upd, err := p.SnapshotClient.SnapshotV1().VolumeSnapshots(pvc.Namespace).Create(context.Background(), &snapshot, metav1.CreateOptions{})
	if err != nil {
		return nil, nil, "", nil, errors.Wrapf(err, "error creating volume snapshot")
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
	operationID := ""
	itemToUpdate := []velero.ResourceIdentifier{}
	if boolptr.IsSetToTrue(backup.Spec.SnapshotMoveData) {
		curLog := p.Log.WithFields(logrus.Fields{
			"source PVC":      fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name),
			"volume snapshot": fmt.Sprintf("%s/%s", upd.Namespace, upd.Name),
		})

		curLog.Info("Starting data movement backup")

		operationID = label.GetValidName(string(backup.GetUID()) + "." + string(pvc.GetUID()))

		snapshotBackup, err := createSnapshotBackup(context.Background(), backup, p.VeleroClient, upd, &pvc, operationID)
		if err != nil {
			curLog.WithError(err).Error("Failed to submit data movement backup")
			util.DeleteVolumeSnapshotIfAny(context.Background(), p.SnapshotClient, upd.Name, upd.Namespace, curLog)

			return nil, nil, "", nil, errors.Wrapf(err, "error creating SnapshotBackup")
		} else {
			itemToUpdate = []velero.ResourceIdentifier{
				{
					GroupResource: schema.GroupResource{
						Group:    "velero.io",
						Resource: "snapshotbackups",
					},
					Namespace: snapshotBackup.Namespace,
					Name:      snapshotBackup.Name,
				},
			}

			curLog.WithField("snapshotBackup name", snapshotBackup.Name).
				WithField("operationID", operationID).
				Info("Data movement backup is submitted successfully")
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
		return nil, nil, "", nil, errors.WithStack(err)
	}

	return &unstructured.Unstructured{Object: pvcMap}, additionalItems, operationID, itemToUpdate, nil
}

func (p *PVCBackupItemAction) Progress(operationID string, backup *velerov1api.Backup) (velero.OperationProgress, error) {
	progress := velero.OperationProgress{}
	if operationID == "" {
		return progress, biav2.InvalidOperationIDError(operationID)
	}

	snapshotBackup, err := getSnapshotBackup(context.Background(), backup, p.VeleroClient, operationID)
	if err != nil {
		return progress, err
	}

	progress.Description = string(snapshotBackup.Status.Phase)
	progress.OperationUnits = "Bytes"
	progress.NCompleted = snapshotBackup.Status.Progress.BytesDone
	progress.NTotal = snapshotBackup.Status.Progress.TotalBytes

	if snapshotBackup.Status.StartTimestamp != nil {
		progress.Started = snapshotBackup.Status.StartTimestamp.Time
	}

	if snapshotBackup.Status.CompletionTimestamp != nil {
		progress.Updated = snapshotBackup.Status.CompletionTimestamp.Time
	}

	if snapshotBackup.Status.Phase == velerov1api.SnapshotBackupPhaseCompleted {
		progress.Completed = true
	} else if snapshotBackup.Status.Phase == velerov1api.SnapshotBackupPhaseFailed {
		progress.Completed = true
		progress.Err = snapshotBackup.Status.Message
	}

	return progress, nil
}

func (p *PVCBackupItemAction) Cancel(operationID string, backup *velerov1api.Backup) error {
	if operationID == "" {
		return biav2.InvalidOperationIDError(operationID)
	}

	snapshotBackup, err := getSnapshotBackup(context.Background(), backup, p.VeleroClient, operationID)
	if err != nil {
		return err
	}

	snapshotBackup.Spec.Cancel = true

	return cancelSnapshotBackup(context.Background(), p.VeleroClient, snapshotBackup)
}

func newSnapshotBackup(backup *velerov1api.Backup, vs *snapshotv1api.VolumeSnapshot,
	pvc *corev1api.PersistentVolumeClaim, operationId string) *velerov1api.SnapshotBackup {
	snapshotBackup := &velerov1api.SnapshotBackup{
		TypeMeta: metav1.TypeMeta{
			APIVersion: velerov1api.SchemeGroupVersion.String(),
			Kind:       "SnapshotBackup",
		},
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
				velerov1api.BackupNameLabel:       label.GetValidName(backup.Name),
				velerov1api.BackupUIDLabel:        string(backup.UID),
				velerov1api.PVCUIDLabel:           string(pvc.UID),
				velerov1api.AsyncOperationIdLabel: operationId,
			},
		},
		Spec: velerov1api.SnapshotBackupSpec{
			SnapshotType: velerov1api.SnapshotTypeCSI,
			CSISnapshot: &velerov1api.CSISnapshotSpec{
				VolumeSnapshot: vs.Name,
				StorageClass:   *pvc.Spec.StorageClassName,
			},
			SourcePVC:             pvc.Name,
			DataMover:             backup.Spec.DataMover,
			BackupStorageLocation: backup.Spec.StorageLocation,
			SourceNamespace:       pvc.Namespace,
			OperationTimeout:      backup.Spec.CSISnapshotTimeout,
		},
	}

	return snapshotBackup
}

func createSnapshotBackup(ctx context.Context, backup *velerov1api.Backup, veleroClient *veleroClientSet.Clientset,
	vs *snapshotv1api.VolumeSnapshot, pvc *corev1api.PersistentVolumeClaim, operationId string) (*velerov1api.SnapshotBackup, error) {
	snapshotBackup := newSnapshotBackup(backup, vs, pvc, operationId)

	snapshotBackup, err := veleroClient.VeleroV1().SnapshotBackups(snapshotBackup.Namespace).Create(ctx, snapshotBackup, metav1.CreateOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error to create SnapshotBackup CR")
	}

	return snapshotBackup, nil
}

func getSnapshotBackup(ctx context.Context, backup *velerov1api.Backup, veleroClient *veleroClientSet.Clientset, operationID string) (*velerov1api.SnapshotBackup, error) {
	listOptions := metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", velerov1api.AsyncOperationIdLabel, operationID)}

	snapshotBackupList, err := veleroClient.VeleroV1().SnapshotBackups(backup.Namespace).List(context.Background(), listOptions)
	if err != nil {
		return nil, errors.Wrap(err, "error to list SnapshotBackup")
	}

	if len(snapshotBackupList.Items) == 0 {
		return nil, errors.Errorf("not found SnapshotBackup for operationID %s", operationID)
	}

	if len(snapshotBackupList.Items) > 1 {
		return nil, errors.Errorf("more than one SnapshotBackup found operationID %s", operationID)
	}

	return &snapshotBackupList.Items[0], nil
}

func cancelSnapshotBackup(ctx context.Context, veleroClient *veleroClientSet.Clientset, ssb *velerov1api.SnapshotBackup) error {
	oldData, err := json.Marshal(ssb)
	if err != nil {
		return errors.Wrap(err, "error marshalling original SnapshotBackup")
	}

	updated := ssb.DeepCopy()
	updated.Spec.Cancel = true

	newData, err := json.Marshal(updated)
	if err != nil {
		return errors.Wrap(err, "error marshalling updated SnapshotBackup")
	}

	patchData, err := jsonpatch.CreateMergePatch(oldData, newData)
	if err != nil {
		return errors.Wrap(err, "error creating json merge patch for SnapshotBackup")
	}

	_, err = veleroClient.VeleroV1().SnapshotBackups(ssb.Namespace).Patch(ctx, ssb.Name, types.MergePatchType, patchData, metav1.PatchOptions{})
	if err != nil {
		return err
	}

	return nil
}
