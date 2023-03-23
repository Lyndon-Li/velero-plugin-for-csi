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
	"encoding/json"
	"fmt"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	corev1api "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"

	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	veleroClientSet "github.com/vmware-tanzu/velero/pkg/generated/clientset/versioned"

	riav2 "github.com/vmware-tanzu/velero/pkg/plugin/velero/restoreitemaction/v2"
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
	Log            logrus.FieldLogger
	Client         *kubernetes.Clientset
	SnapshotClient *snapshotterClientSet.Clientset
	VeleroClient   *veleroClientSet.Clientset
}

// Name is required to implement the interface, but the Velero pod does not delegate this
// method -- it's used to tell velero what name it was registered under. The plugin implementation
// must define it, but it will never actually be called.
func (p *PVCRestoreItemAction) Name() string {
	return "PVCRestoreItemAction"
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
	var pvc, pvcFromBackup corev1api.PersistentVolumeClaim
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(input.Item.UnstructuredContent(), &pvc)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	err = runtime.DefaultUnstructuredConverter.FromUnstructured(input.ItemFromBackup.UnstructuredContent(), &pvcFromBackup)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	p.Log.Infof("Starting PVCRestoreItemAction for PVC %s/%s", pvc.Namespace, pvc.Name)
	if boolptr.IsSetToFalse(input.Restore.Spec.RestorePVs) {
		p.Log.Infof("Restore did not request for PVs to be restored %s/%s", input.Restore.Namespace, input.Restore.Name)
		return &velero.RestoreItemActionExecuteOutput{SkipRestore: true}, nil
	}

	removePVCAnnotations(&pvc,
		[]string{AnnBindCompleted, AnnBoundByController, AnnStorageProvisioner, AnnBetaStorageProvisioner, AnnSelectedNode})

	// If cross-namespace restore is configured, change the namespace
	// for PVC object to be restored
	if val, ok := input.Restore.Spec.NamespaceMapping[pvc.GetNamespace()]; ok {
		pvc.SetNamespace(val)
	}

	operationID := ""
	if boolptr.IsSetToTrue(input.Restore.Spec.SnapshotMoveData) {
		curLog := p.Log.WithField("target PVC", fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name))
		curLog.Info("Starting data movement restore")

		operationID = label.GetValidName(string(input.Restore.GetUID()) + "." + string(pvcFromBackup.GetUID()))

		snapshotRestore, err := restoreFromDataMovement(context.Background(), p.VeleroClient, p.Client, input.Restore, &pvc, operationID, p.Log)
		if err != nil {
			curLog.WithError(err).Error("Failed to submit data movement restore")
			return nil, errors.WithStack(err)
		} else {
			curLog.WithField("snapshotRestore name", snapshotRestore.Name).Info("Data movement restore is submitted successfully")
		}
	} else {
		err = restoreFromVolumeSnapshot(&pvc, p.SnapshotClient, p.Log)
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
		OperationID: operationID,
	}, nil
}

func (p *PVCRestoreItemAction) Progress(operationID string, restore *velerov1api.Restore) (velero.OperationProgress, error) {
	progress := velero.OperationProgress{}

	if operationID == "" {
		return progress, riav2.InvalidOperationIDError(operationID)
	}

	snapshotRestore, err := getSnapshotRestore(context.Background(), restore, p.VeleroClient, operationID)
	if err != nil {
		return progress, err
	}

	progress.Description = string(snapshotRestore.Status.Phase)
	progress.OperationUnits = "Bytes"
	progress.NCompleted = snapshotRestore.Status.Progress.BytesDone
	progress.NTotal = snapshotRestore.Status.Progress.TotalBytes

	if snapshotRestore.Status.StartTimestamp != nil {
		progress.Started = snapshotRestore.Status.StartTimestamp.Time
	}

	if snapshotRestore.Status.CompletionTimestamp != nil {
		progress.Updated = snapshotRestore.Status.CompletionTimestamp.Time
	}

	if snapshotRestore.Status.Phase == velerov1api.SnapshotRestorePhaseCompleted {
		progress.Completed = true
	} else if snapshotRestore.Status.Phase == velerov1api.SnapshotRestorePhaseFailed {
		progress.Completed = true
		progress.Err = snapshotRestore.Status.Message
	}

	return progress, nil
}

func (p *PVCRestoreItemAction) Cancel(operationID string, restore *velerov1api.Restore) error {
	if operationID == "" {
		return riav2.InvalidOperationIDError(operationID)
	}

	snapshotRestore, err := getSnapshotRestore(context.Background(), restore, p.VeleroClient, operationID)
	if err != nil {
		return err
	}

	snapshotRestore.Spec.Cancel = true

	return cancelSnapshotRestore(context.Background(), p.VeleroClient, snapshotRestore)
}

func (p *PVCRestoreItemAction) AreAdditionalItemsReady(additionalItems []velero.ResourceIdentifier, restore *velerov1api.Restore) (bool, error) {
	return true, nil
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

func restoreFromDataMovement(ctx context.Context, veleroClient *veleroClientSet.Clientset, kubeClient *kubernetes.Clientset,
	restore *velerov1api.Restore, pvc *corev1api.PersistentVolumeClaim, operationId string, log logrus.FieldLogger) (*velerov1api.SnapshotRestore, error) {
	backupResult, err := getSnapshotBackupResult(ctx, restore, kubeClient)
	if err != nil {
		return nil, errors.Wrapf(err, "error to get backup info for restore %s", restore.Name)
	}

	pvc.Spec.VolumeName = ""
	if pvc.Spec.Selector == nil {
		pvc.Spec.Selector = &metav1.LabelSelector{}
	}

	if pvc.Spec.Selector.MatchLabels == nil {
		pvc.Spec.Selector.MatchLabels = make(map[string]string)
	}

	pvc.Spec.Selector.MatchLabels[velerov1api.DynamicPVRestoreLabel] = label.GetValidName(fmt.Sprintf("%s.%s", pvc.Namespace, pvc.Name))

	snapshotRestore := newSnapshotRestore(restore, backupResult, pvc, operationId)
	snapshotRestore, err = veleroClient.VeleroV1().SnapshotRestores(snapshotRestore.Namespace).Create(ctx, snapshotRestore, metav1.CreateOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error to create SnapshotRestore CR")
	}

	log.WithField("SnapshotRestore CR", snapshotRestore.Name).Info("SnapshotRestore CR is created")

	return snapshotRestore, nil
}

func newSnapshotRestore(restore *velerov1api.Restore, backupResult *velerov1api.SnapshotBackupResult,
	pvc *corev1api.PersistentVolumeClaim, operationId string) *velerov1api.SnapshotRestore {
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
				velerov1api.RestoreNameLabel:      label.GetValidName(restore.Name),
				velerov1api.RestoreUIDLabel:       string(restore.UID),
				velerov1api.AsyncOperationIdLabel: operationId,
			},
		},
		Spec: velerov1api.SnapshotRestoreSpec{
			TargetVolume: velerov1api.TargetVolumeSpec{
				PVC:          pvc.Name,
				Namespace:    pvc.Namespace,
				PVLabel:      pvc.Spec.Selector.MatchLabels[velerov1api.DynamicPVRestoreLabel],
				StorageClass: *pvc.Spec.StorageClassName,
				Resources:    pvc.Spec.Resources,
			},
			BackupStorageLocation: backupResult.BackupStorageLocation,
			DataMover:             backupResult.DataMover,
			SnapshotID:            backupResult.SnapshotID,
			SourceNamespace:       backupResult.SourceNamespace,
			OperationTimeout:      restore.Spec.ItemOperationTimeout,
		},
	}

	return snapshotRestore
}

func getSnapshotBackupResult(ctx context.Context, restore *velerov1api.Restore,
	kubeClient *kubernetes.Clientset) (*velerov1api.SnapshotBackupResult, error) {
	cmList, err := kubeClient.CoreV1().ConfigMaps(restore.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", velerov1api.BackupNameLabel, label.GetValidName(restore.Spec.BackupName)),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "error to get snapshot backup result cm with name %s", restore.Spec.BackupName)
	}

	defer func() {
		for _, item := range cmList.Items {
			kubeClient.CoreV1().ConfigMaps(item.Namespace).Delete(ctx, item.Name, metav1.DeleteOptions{})
		}
	}()

	if len(cmList.Items) == 0 {
		return nil, errors.Errorf("no snapshot backup result cm found for %s", restore.Spec.BackupName)
	}

	if len(cmList.Items) > 1 {
		return nil, errors.Errorf("multiple snapshot backup result cms found for %s", restore.Spec.BackupName)
	}

	jsonBytes, exist := cmList.Items[0].Data[string(restore.UID)]
	if !exist {
		return nil, errors.Errorf("no snapshot backup result found with restore key %s, restore %s", string(restore.UID), restore.Name)
	}

	result := velerov1api.SnapshotBackupResult{}
	err = json.Unmarshal([]byte(jsonBytes), &result)
	if err != nil {
		return nil, errors.Errorf("error to unmarshal backup result, restore UID %s, restore name %s", string(restore.UID), restore.Name)
	}

	return &result, nil
}

func getSnapshotRestore(ctx context.Context, restore *velerov1api.Restore, veleroClient *veleroClientSet.Clientset, operationID string) (*velerov1api.SnapshotRestore, error) {
	listOptions := metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", velerov1api.AsyncOperationIdLabel, operationID)}

	snapshotRestoreList, err := veleroClient.VeleroV1().SnapshotRestores(restore.Namespace).List(context.Background(), listOptions)
	if err != nil {
		return nil, errors.Wrap(err, "error to list SnapshotRestore")
	}

	if len(snapshotRestoreList.Items) == 0 {
		return nil, errors.Errorf("not found SnapshotRestore for operationID %s", operationID)
	}

	if len(snapshotRestoreList.Items) > 1 {
		return nil, errors.Errorf("more than one SnapshotRestore found operationID %s", operationID)
	}

	return &snapshotRestoreList.Items[0], nil
}

func cancelSnapshotRestore(ctx context.Context, veleroClient *veleroClientSet.Clientset, ssr *velerov1api.SnapshotRestore) error {
	oldData, err := json.Marshal(ssr)
	if err != nil {
		return errors.Wrap(err, "error marshalling original SnapshotRestore")
	}

	updated := ssr.DeepCopy()
	updated.Spec.Cancel = true

	newData, err := json.Marshal(updated)
	if err != nil {
		return errors.Wrap(err, "error marshalling updated SnapshotRestore")
	}

	patchData, err := jsonpatch.CreateMergePatch(oldData, newData)
	if err != nil {
		return errors.Wrap(err, "error creating json merge patch for SnapshotRestore")
	}

	_, err = veleroClient.VeleroV1().SnapshotRestores(ssr.Namespace).Patch(ctx, ssr.Name, types.MergePatchType, patchData, metav1.PatchOptions{})
	if err != nil {
		return err
	}

	return nil
}
