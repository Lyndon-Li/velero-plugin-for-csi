/*
Copyright 2020 the Velero contributors.

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
	"strings"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	biav2 "github.com/vmware-tanzu/velero/pkg/plugin/velero/backupitemaction/v2"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"

	"github.com/google/uuid"
	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	velerov2alpha1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v2alpha1"
	veleroClientSet "github.com/vmware-tanzu/velero/pkg/generated/clientset/versioned"
	corev1api "k8s.io/api/core/v1"
)

// VolumeSnapshotBackupItemAction is a backup item action plugin to backup
// CSI VolumeSnapshot objects using Velero
type VolumeSnapshotBackupItemAction struct {
	Log            logrus.FieldLogger
	Client         kubernetes.Interface
	SnapshotClient snapshotterClientSet.Interface
	VeleroClient   veleroClientSet.Interface
}

// AppliesTo returns information indicating that the VolumeSnapshotBackupItemAction should be invoked to backup volumesnapshots.
func (p *VolumeSnapshotBackupItemAction) AppliesTo() (velero.ResourceSelector, error) {
	p.Log.Debug("VolumeSnapshotBackupItemAction AppliesTo")

	return velero.ResourceSelector{
		IncludedResources: []string{"volumesnapshots.snapshot.storage.k8s.io"},
	}, nil
}

// Execute backs up a CSI volumesnapshot object and captures, as labels and annotations, information from its associated volumesnapshotcontents such as CSI driver name, storage snapshot handle
// and namespace and name of the snapshot delete secret, if any. It returns the volumesnapshotclass and the volumesnapshotcontents as additional items to be backed up.
func (p *VolumeSnapshotBackupItemAction) Execute(item runtime.Unstructured, backup *velerov1api.Backup) (runtime.Unstructured, []velero.ResourceIdentifier,
	string, []velero.ResourceIdentifier, error) {
	p.Log.Infof("Executing VolumeSnapshotBackupItemAction")

	var vs snapshotv1api.VolumeSnapshot
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.UnstructuredContent(), &vs); err != nil {
		return nil, nil, "", nil, errors.WithStack(err)
	}

	additionalItems := []velero.ResourceIdentifier{
		{
			GroupResource: kuberesource.VolumeSnapshotClasses,
			Name:          *vs.Spec.VolumeSnapshotClassName,
		},
	}

	// determine if we are backing up a volumesnapshot that was created by velero while performing backup of a
	// CSI backed PVC.
	// For volumesnapshots that were created during the backup of a CSI backed PVC, we will wait for the volumecontents to
	// be available.
	// For volumesnapshots created outside of velero, we expect the volumesnapshotcontent to be available prior to backing up
	// the volumesnapshot. In case of a failure, backup should be re-attempted after the CSI driver has reconciled the volumesnapshot.
	// existence of the velerov1api.BackupNameLabel indicates that the volumesnapshot was created while backing up a
	// CSI backed PVC.

	// We want to await reconciliation of only those volumesnapshots created during the ongoing backup.
	// For this we will wait only if the backup label exists on the volumesnapshot object and the
	// backup name is the same as that of the value of the backupLabel
	backupOngoing := vs.Labels[velerov1api.BackupNameLabel] == label.GetValidName(backup.Name)

	p.Log.Infof("Getting VolumesnapshotContent for Volumesnapshot %s/%s", vs.Namespace, vs.Name)

	vsc, err := util.GetVolumeSnapshotContentForVolumeSnapshot(&vs, p.SnapshotClient.SnapshotV1(), p.Log, backupOngoing, backup.Spec.CSISnapshotTimeout.Duration)
	if err != nil {
		util.CleanupVolumeSnapshot(&vs, p.SnapshotClient.SnapshotV1(), p.Log)
		return nil, nil, "", nil, errors.WithStack(err)
	}

	if backup.Status.Phase == velerov1api.BackupPhaseFinalizing || backup.Status.Phase == velerov1api.BackupPhaseFinalizingPartiallyFailed {
		p.Log.WithField("Backup", fmt.Sprintf("%s/%s", backup.Namespace, backup.Name)).
			WithField("BackupPhase", backup.Status.Phase).Debugf("Clean VolumeSnapshots.")
		util.DeleteVolumeSnapshot(vs, *vsc, backup, p.SnapshotClient.SnapshotV1(), p.Log)
		return item, nil, "", nil, nil
	}

	annotations := make(map[string]string)

	var pvc *corev1api.PersistentVolumeClaim
	if backupOngoing {
		pvc, err = util.GetPVCFromVolumeSnapshot(context.TODO(), p.Client.CoreV1(), &vs)
		if err != nil {
			return nil, nil, "", nil, errors.Wrapf(err, "error to get PVC from VS %s/%s", vs.Namespace, vs.Name)
		}

		annotations[util.SnapshotVolumeLabel] = string(pvc.UID)
	}

	uuid, err := uuid.NewRandom()
	if err != nil {
		return nil, nil, "", nil, errors.WithStack(err)
	}

	snapshotAlias := uuid.String()

	if vsc != nil {
		// when we are backing up volumesnapshots created outside of velero, we will not
		// await volumesnapshot reconciliation and in this case GetVolumeSnapshotContentForVolumeSnapshot
		// may not find the associated volumesnapshotcontents to add to the backup.
		// This is not an error encountered in the backup process. So we add the volumesnapshotcontent
		// to the backup only if one is found.
		annotations[util.CSIVSCDeletionPolicy] = string(vsc.Spec.DeletionPolicy)

		if vsc.Status != nil {
			if vsc.Status.SnapshotHandle != nil {
				// Capture storage provider snapshot handle and CSI driver name
				// to be used on restore to create a static volumesnapshotcontent that will be the source of the volumesnapshot.
				annotations[util.VolumeSnapshotHandleAnnotation] = *vsc.Status.SnapshotHandle
				annotations[util.CSIDriverNameAnnotation] = vsc.Spec.Driver
			}
			if vsc.Status.RestoreSize != nil {
				annotations[util.VolumeSnapshotRestoreSize] = resource.NewQuantity(*vsc.Status.RestoreSize, resource.BinarySI).String()
			}
		}

		if backupOngoing {
			p.Log.Infof("Patching volumesnapshotcontent %s with velero BackupNameLabel", vsc.Name)
			// If we created the volumesnapshotcontent object during this ongoing backup, we would have created it with a DeletionPolicy of Retain.
			// But, we want to retain these volumesnapshotcontent ONLY for the lifetime of the backup. To that effect, during velero backup
			// deletion, we will update the DeletionPolicy of the volumesnapshotcontent and then delete the VolumeSnapshot object which will
			// cascade delete the volumesnapshotcontent and the associated snapshot in the storage provider (handled by the CSI driver and
			// the CSI common controller).
			// However, in the event that the Volumesnapshot object is deleted outside of the backup deletion process, it is possible that
			// the dynamically created volumesnapshotcontent object will be left as an orphaned and non-discoverable resource in the cluster as well
			// as in the storage provider. To avoid piling up of such orphaned resources, we will want to discover and delete the dynamically created
			// volumesnapshotcontents. We do that by adding the "velero.io/backup-name" label on the volumesnapshotcontent.
			// Further, we want to add this label only on volumesnapshotcontents that were created during an ongoing velero backup.

			pb := []byte(fmt.Sprintf(`{"metadata":{"labels":{"%s":"%s", "%s":"%s", "%s":"%s"}}}`, velerov1api.BackupNameLabel, label.GetValidName(backup.Name), util.SnapshotAliasLabel, label.GetValidName(snapshotAlias), util.SnapshotVolumeLabel, label.GetValidName(string(pvc.UID))))
			if _, vscPatchError := p.SnapshotClient.SnapshotV1().VolumeSnapshotContents().Patch(context.TODO(), vsc.Name, types.MergePatchType, pb, metav1.PatchOptions{}); vscPatchError != nil {
				p.Log.Warnf("Failed to patch volumesnapshotcontent %s: %v", vsc.Name, vscPatchError)
			}

			if boolptr.IsSetToTrue(backup.Spec.SnapshotMoveData) {
				if util.IsVolumeSnapshotContentHasDeleteSecret(vsc) {
					additionalItems = append(additionalItems, velero.ResourceIdentifier{
						GroupResource: schema.GroupResource{Group: "", Resource: "secrets"},
						Name:          vsc.Annotations[util.CSIDeleteSnapshotSecretName],
						Namespace:     vsc.Annotations[util.CSIDeleteSnapshotSecretNamespace],
					})
				}
			} else {
				additionalItems = append(additionalItems, velero.ResourceIdentifier{
					GroupResource: kuberesource.VolumeSnapshotContents,
					Name:          vsc.Name,
				})
			}
		}
	}

	operationID := ""
	var itemToUpdate []velero.ResourceIdentifier

	// Only return Async operation for VSC created for this backup.
	if backupOngoing {
		if boolptr.IsSetToTrue(backup.Spec.SnapshotMoveData) {
			operationID, itemToUpdate, err = backupFromDataMover(context.TODO(), p.VeleroClient, p.Client, p.SnapshotClient, &vs, vsc, backup, pvc, annotations, snapshotAlias, p.Log)
			if err != nil {
				return nil, nil, "", nil, errors.WithStack(err)
			}
		} else {
			// The operationID is of the form <namespace>/<volumesnapshot-name>/<started-time>
			operationID = vs.Namespace + "/" + vs.Name + "/" + time.Now().Format(time.RFC3339)
			itemToUpdate = []velero.ResourceIdentifier{
				{
					GroupResource: kuberesource.VolumeSnapshots,
					Namespace:     vs.Namespace,
					Name:          vs.Name,
				},
			}
		}
	}

	// Before applying the BIA v2, the in-cluster VS state is not persisted into backup.
	// After the change, because the final state of VS will be stored in backup as the
	// result of async operation result, need to patch the annotations into VS to work,
	// because restore will check the annotations information.
	pb := "{\"metadata\":{\"annotations\":{"
	for k, v := range annotations {
		pb += fmt.Sprintf("\"%s\":\"%s\",", k, v)
	}
	pb = strings.Trim(pb, ",")
	pb += "}}}"
	if _, err := p.SnapshotClient.SnapshotV1().VolumeSnapshots(vs.Namespace).Patch(context.TODO(),
		vs.Name, types.MergePatchType, []byte(pb), metav1.PatchOptions{}); err != nil {
		p.Log.Errorf("Fail to patch volumesnapshot with content %s: %s.", pb, err.Error())
		return nil, nil, "", nil, errors.WithStack(err)
	}

	p.Log.Infof("Returning from VolumeSnapshotBackupItemAction with %d additionalItems to backup", len(additionalItems))
	for _, ai := range additionalItems {
		p.Log.Debugf("%s: %s", ai.GroupResource.String(), ai.Name)
	}

	annotations[util.MustIncludeAdditionalItemAnnotation] = "true"
	// save newly applied annotations into the backed-up volumesnapshot item
	util.AddAnnotations(&vs.ObjectMeta, annotations)

	vsMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&vs)
	if err != nil {
		return nil, nil, "", nil, errors.WithStack(err)
	}

	return &unstructured.Unstructured{Object: vsMap}, additionalItems, operationID, itemToUpdate, nil
}

func (p *VolumeSnapshotBackupItemAction) Name() string {
	return "VolumeSnapshotBackupItemAction"
}

func (p *VolumeSnapshotBackupItemAction) Progress(operationID string, backup *velerov1api.Backup) (velero.OperationProgress, error) {
	progress := velero.OperationProgress{}
	if operationID == "" {
		return progress, biav2.InvalidOperationIDError(operationID)
	}

	if strings.HasPrefix(operationID, string(velerov1api.AsyncOperationIDPrefixDataUpload)) {
		dataUpload, err := getDataUpload(context.Background(), backup, p.VeleroClient, operationID)
		if err != nil {
			p.Log.Errorf("fail to get DataUpload for backup %s/%s: %s", backup.Namespace, backup.Name, err.Error())
			return progress, err
		}
		if dataUpload.Status.Phase == velerov2alpha1.DataUploadPhaseNew || dataUpload.Status.Phase == "" {
			p.Log.Debugf("DataUpload is still not processed yet. Skip progress update.")
			return progress, nil
		}

		progress.Description = string(dataUpload.Status.Phase)
		progress.OperationUnits = "Bytes"
		progress.NCompleted = dataUpload.Status.Progress.BytesDone
		progress.NTotal = dataUpload.Status.Progress.TotalBytes

		if dataUpload.Status.StartTimestamp != nil {
			progress.Started = dataUpload.Status.StartTimestamp.Time
		}

		if dataUpload.Status.CompletionTimestamp != nil {
			progress.Updated = dataUpload.Status.CompletionTimestamp.Time
		}

		if dataUpload.Status.Phase == velerov2alpha1.DataUploadPhaseCompleted {
			progress.Completed = true
		} else if dataUpload.Status.Phase == velerov2alpha1.DataUploadPhaseFailed {
			progress.Completed = true
			progress.Err = dataUpload.Status.Message
		} else if dataUpload.Status.Phase == velerov2alpha1.DataUploadPhaseCanceled {
			progress.Completed = true
			progress.Err = "DataUpload is canceled"
		}
	} else {
		// The operationID is of the form <namespace>/<volumesnapshot-name>/<started-time>
		operationIDParts := strings.Split(operationID, "/")
		if len(operationIDParts) != 3 {
			p.Log.Errorf("invalid operation ID %s", operationID)
			return progress, biav2.InvalidOperationIDError(operationID)
		}
		var err error
		if progress.Started, err = time.Parse(time.RFC3339, operationIDParts[2]); err != nil {
			p.Log.Errorf("error parsing operation ID's StartedTime part into time %s: %s", operationID, err.Error())
			return progress, errors.WithStack(err)
		}

		_, snapshotClient, err := util.GetClients()
		if err != nil {
			return progress, errors.WithStack(err)
		}

		vs, err := snapshotClient.SnapshotV1().VolumeSnapshots(operationIDParts[0]).Get(context.Background(), operationIDParts[1], metav1.GetOptions{})
		if err != nil {
			p.Log.Errorf("error getting volumesnapshot %s/%s: %s", operationIDParts[0], operationIDParts[1], err.Error())
			return progress, errors.WithStack(err)
		}

		if vs.Status == nil {
			p.Log.Debugf("VolumeSnapshot %s/%s has an empty status. Skip progress update.", vs.Namespace, vs.Name)
			return progress, nil
		}

		if boolptr.IsSetToTrue(vs.Status.ReadyToUse) {
			progress.Completed = true
		} else if vs.Status.Error != nil {
			errorMessage := ""
			if vs.Status.Error.Message != nil {
				errorMessage = *vs.Status.Error.Message
			}
			p.Log.Warnf("VolumeSnapshot has a temporary error %s. Snapshot controller will retry later.", errorMessage)
		}
	}

	return progress, nil
}

func (p *VolumeSnapshotBackupItemAction) Cancel(operationID string, backup *velerov1api.Backup) error {
	if operationID == "" {
		return biav2.InvalidOperationIDError(operationID)
	}

	if strings.HasPrefix(operationID, string(velerov1api.AsyncOperationIDPrefixDataUpload)) {
		dataUpload, err := getDataUpload(context.Background(), backup, p.VeleroClient, operationID)
		if err != nil {
			p.Log.Errorf("fail to get DataUpload for backup %s/%s: %s", backup.Namespace, backup.Name, err.Error())
			return err
		}

		return cancelDataUpload(context.Background(), p.VeleroClient, dataUpload)
	} else {
		// CSI Specification doesn't support canceling a snapshot creation.
		return nil
	}
}

func backupFromDataMover(ctx context.Context, veleroClient veleroClientSet.Interface, kubeClient kubernetes.Interface,
	snapshotClient snapshotterClientSet.Interface, upd *snapshotv1api.VolumeSnapshot, snapCont *snapshotv1api.VolumeSnapshotContent,
	backup *velerov1api.Backup, pvc *corev1api.PersistentVolumeClaim, vsAnnotation map[string]string,
	snapshotAlias string, logger logrus.FieldLogger) (string, []velero.ResourceIdentifier, error) {
	operationID := label.GetValidName(string(velerov1api.AsyncOperationIDPrefixDataUpload) + string(backup.UID) + "." + string(pvc.UID))

	dataUploadLog := logger.WithFields(logrus.Fields{
		"Source PVC":     fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name),
		"VolumeSnapshot": fmt.Sprintf("%s/%s", upd.Namespace, upd.Name),
		"Operation ID":   operationID,
		"Backup":         backup.Name,
	})

	dataUploadLog.Info("Starting data upload of backup")

	retainSnapshot := detectSnapshotRetain(context.TODO(), snapshotClient, string(pvc.UID), backup.Spec.SnapshotsToRetain, dataUploadLog)
	dataUpload, err := createDataUpload(context.Background(), backup, veleroClient, upd, pvc, operationID, retainSnapshot)
	if err != nil {
		dataUploadLog.WithError(err).Error("failed to submit DataUpload")
		util.DeleteVolumeSnapshotIfAny(context.Background(), snapshotClient, *upd, dataUploadLog)

		return "", nil, errors.Wrapf(err, "error creating DataUpload")
	}

	itemToUpdate := []velero.ResourceIdentifier{
		{
			GroupResource: schema.GroupResource{
				Group:    "velero.io",
				Resource: "datauploads",
			},
			Namespace: dataUpload.Namespace,
			Name:      dataUpload.Name,
		},
	}

	if retainSnapshot {
		itemToUpdate = append(itemToUpdate, velero.ResourceIdentifier{
			GroupResource: kuberesource.VolumeSnapshotContents,
			LabelSelector: util.SnapshotAliasLabel + "=" + snapshotAlias,
		})
	}

	// Set the DataUploadNameLabel, which is used for restore to let CSI plugin check whether
	// it should handle the volume. If volume is CSI migration, PVC doesn't have the annotation.
	vsAnnotation[util.DataUploadNameAnnotation] = dataUpload.Namespace + "/" + dataUpload.Name

	dataUploadLog.Info("DataUpload is submitted successfully.")

	return operationID, itemToUpdate, nil
}

func newDataUpload(backup *velerov1api.Backup, vs *snapshotv1api.VolumeSnapshot,
	pvc *corev1api.PersistentVolumeClaim, operationID string, retainSnapshot bool) *velerov2alpha1.DataUpload {
	dataUpload := &velerov2alpha1.DataUpload{
		TypeMeta: metav1.TypeMeta{
			APIVersion: velerov2alpha1.SchemeGroupVersion.String(),
			Kind:       "DataUpload",
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
				velerov1api.AsyncOperationIDLabel: operationID,
			},
		},
		Spec: velerov2alpha1.DataUploadSpec{
			SnapshotType: velerov2alpha1.SnapshotTypeCSI,
			CSISnapshot: &velerov2alpha1.CSISnapshotSpec{
				VolumeSnapshot: vs.Name,
				StorageClass:   *pvc.Spec.StorageClassName,
			},
			SourcePVC:             pvc.Name,
			DataMover:             backup.Spec.DataMover,
			BackupStorageLocation: backup.Spec.StorageLocation,
			SourceNamespace:       pvc.Namespace,
			OperationTimeout:      backup.Spec.CSISnapshotTimeout,
			RetainSnapshot:        retainSnapshot,
		},
	}

	return dataUpload
}

func createDataUpload(ctx context.Context, backup *velerov1api.Backup, veleroClient veleroClientSet.Interface,
	vs *snapshotv1api.VolumeSnapshot, pvc *corev1api.PersistentVolumeClaim, operationID string, retainSnapshot bool) (*velerov2alpha1.DataUpload, error) {
	dataUpload := newDataUpload(backup, vs, pvc, operationID, retainSnapshot)

	dataUpload, err := veleroClient.VeleroV2alpha1().DataUploads(dataUpload.Namespace).Create(ctx, dataUpload, metav1.CreateOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "fail to create DataUpload CR")
	}

	return dataUpload, err
}

func getDataUpload(ctx context.Context, backup *velerov1api.Backup,
	veleroClient veleroClientSet.Interface, operationID string) (*velerov2alpha1.DataUpload, error) {
	listOptions := metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", velerov1api.AsyncOperationIDLabel, operationID)}

	dataUploadList, err := veleroClient.VeleroV2alpha1().DataUploads(backup.Namespace).List(context.Background(), listOptions)
	if err != nil {
		return nil, errors.Wrapf(err, "error to list DataUpload")
	}

	if len(dataUploadList.Items) == 0 {
		return nil, errors.Errorf("not found DataUpload for operationID %s", operationID)
	}

	if len(dataUploadList.Items) > 1 {
		return nil, errors.Errorf("more than one DataUpload found operationID %s", operationID)
	}

	return &dataUploadList.Items[0], nil
}

func cancelDataUpload(ctx context.Context, veleroClient veleroClientSet.Interface,
	dataUpload *velerov2alpha1.DataUpload) error {
	oldData, err := json.Marshal(dataUpload)
	if err != nil {
		return errors.Wrap(err, "error marshalling original DataUpload")
	}

	updatedDataUpload := dataUpload.DeepCopy()
	updatedDataUpload.Spec.Cancel = true

	newData, err := json.Marshal(updatedDataUpload)
	if err != nil {
		return errors.Wrap(err, "err marshalling updated DataUpload")
	}

	patchData, err := jsonpatch.CreateMergePatch(oldData, newData)
	if err != nil {
		return errors.Wrap(err, "error creating patch data for DataUpload")
	}

	_, err = veleroClient.VeleroV2alpha1().DataUploads(dataUpload.Namespace).Patch(ctx, dataUpload.Name, types.MergePatchType, patchData, metav1.PatchOptions{})
	if err != nil {
		return errors.Wrap(err, "error patch DataUpload")
	}

	return nil
}

func detectSnapshotRetain(ctx context.Context, snapshotClient snapshotterClientSet.Interface, pvcID string, limit int, logger logrus.FieldLogger) bool {
	labelSelector := util.SnapshotVolumeLabel + "=" + pvcID
	listItem, err := snapshotClient.SnapshotV1().VolumeSnapshotContents().List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		logger.WithError(err).Warnf("Failed to list snapshot contents with label %s", labelSelector)
		return false
	}

	if len(listItem.Items) > limit {
		logger.Infof("Don't need to retain snapshots, limit %v, current %v", limit, len(listItem.Items))
		return false
	}

	logger.Infof("Need to retain snapshots, limit %v, current %v", limit, len(listItem.Items))
	return true
}
