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
	"sort"
	"time"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1api "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"

	"github.com/vmware-tanzu/velero-plugin-for-csi/internal/util"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	veleroClientSet "github.com/vmware-tanzu/velero/pkg/generated/clientset/versioned"
	"github.com/vmware-tanzu/velero/pkg/kuberesource"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/plugin/velero"
	biav2 "github.com/vmware-tanzu/velero/pkg/plugin/velero/backupitemaction/v2"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"
)

// PVCBackupItemAction is a backup item action plugin for Velero.
type PVCBackupItemAction struct {
	Log            logrus.FieldLogger
	Client         kubernetes.Interface
	SnapshotClient snapshotterClientSet.Interface
	VeleroClient   veleroClientSet.Interface
}

// const (
// 	snapshotAliasLabel  = "velero.io/snapshot-alias-name"
// 	snapshotVolumeLabel = "velero.io/snapshot-volume"
// )

// AppliesTo returns information indicating that the PVCBackupItemAction should be invoked to backup PVCs.
func (p *PVCBackupItemAction) AppliesTo() (velero.ResourceSelector, error) {
	p.Log.Debug("PVCBackupItemAction AppliesTo")

	return velero.ResourceSelector{
		IncludedResources: []string{"persistentvolumeclaims"},
	}, nil
}

// Execute recognizes PVCs backed by volumes provisioned by CSI drivers with volumesnapshotting capability and creates snapshots of the
// underlying PVs by creating volumesnapshot CSI API objects that will trigger the CSI driver to perform the snapshot operation on the volume.
func (p *PVCBackupItemAction) Execute(item runtime.Unstructured, backup *velerov1api.Backup) (runtime.Unstructured, []velero.ResourceIdentifier, string, []velero.ResourceIdentifier, error) {
	p.Log.Info("Starting PVCBackupItemAction")

	// Do nothing if volume snapshots have not been requested in this backup
	if boolptr.IsSetToFalse(backup.Spec.SnapshotVolumes) {
		p.Log.Infof("Volume snapshotting not requested for backup %s/%s", backup.Namespace, backup.Name)
		return item, nil, "", nil, nil
	}

	if backup.Status.Phase == velerov1api.BackupPhaseFinalizing ||
		backup.Status.Phase == velerov1api.BackupPhaseFinalizingPartiallyFailed {
		p.Log.WithFields(
			logrus.Fields{
				"Backup": fmt.Sprintf("%s/%s", backup.Namespace, backup.Name),
				"Phase":  backup.Status.Phase,
			},
		).Debug("Backup is in finalizing phase. Skip this PVC.")
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

		util.AddAnnotations(&pvc.ObjectMeta, map[string]string{
			util.SkippedNoCSIPVAnnotation: "true",
		})
		data, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pvc)
		return &unstructured.Unstructured{Object: data}, nil, "", nil, err
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
		return item, nil, "", nil, errors.Errorf("Cannot snapshot PVC %s/%s, PVC has no storage class.", pvc.Namespace, pvc.Name)
	}

	p.Log.Infof("Fetching storage class for PV %s", *pvc.Spec.StorageClassName)
	storageClass, err := p.Client.StorageV1().StorageClasses().Get(context.TODO(), *pvc.Spec.StorageClassName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, "", nil, errors.Wrap(err, "error getting storage class")
	}
	p.Log.Debugf("Fetching volumesnapshot class for %s", storageClass.Provisioner)
	snapshotClass, err := util.GetVolumeSnapshotClass(storageClass.Provisioner, backup, &pvc, p.Log, p.SnapshotClient.SnapshotV1())
	if err != nil {
		return nil, nil, "", nil, errors.Wrapf(err, "failed to get volumesnapshotclass for storageclass %s", storageClass.Name)
	}
	p.Log.Infof("volumesnapshot class=%s", snapshotClass.Name)

	if boolptr.IsSetToTrue(backup.Spec.SnapshotMoveData) {
		limit, err := util.GetRetainSnapshotLimit(backup)
		if err != nil {
			p.Log.WithError(err).Error("Failed to get retain snapshot limit from backup")
			return nil, nil, "", nil, errors.Wrapf(err, "error to get retain snapshot limit from backup")
		}

		if limit > 0 {
			if err := gcRetainedSnapshots(context.TODO(), p.SnapshotClient, string(pvc.UID), limit, backup.Spec.CSISnapshotTimeout.Duration, p.Log); err != nil {
				p.Log.WithError(err).Error("Failed to gc retained snapshots")
				return nil, nil, "", nil, errors.Wrapf(err, "error to gc retained snapshots")
			}
		}
	}

	vsLabels := map[string]string{}
	for k, v := range pvc.ObjectMeta.Labels {
		vsLabels[k] = v
	}
	vsLabels[velerov1api.BackupNameLabel] = label.GetValidName(backup.Name)

	// Craft the snapshot object to be created
	snapshot := snapshotv1api.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "velero-" + pvc.Name + "-",
			Namespace:    pvc.Namespace,
			Labels:       vsLabels,
		},
		Spec: snapshotv1api.VolumeSnapshotSpec{
			Source: snapshotv1api.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvc.Name,
			},
			VolumeSnapshotClassName: &snapshotClass.Name,
		},
	}

	upd, err := p.SnapshotClient.SnapshotV1().VolumeSnapshots(pvc.Namespace).Create(context.TODO(), &snapshot, metav1.CreateOptions{})
	if err != nil {
		return nil, nil, "", nil, errors.Wrapf(err, "error creating volume snapshot")
	}
	p.Log.Infof("Created volumesnapshot %s", fmt.Sprintf("%s/%s", upd.Namespace, upd.Name))

	labels := map[string]string{
		util.VolumeSnapshotLabel:    upd.Name,
		velerov1api.BackupNameLabel: backup.Name,
	}

	annotations := map[string]string{
		util.VolumeSnapshotLabel:                 upd.Name,
		util.MustIncludeAdditionalItemAnnotation: "true",
	}

	additionalItems := []velero.ResourceIdentifier{
		{
			GroupResource: kuberesource.VolumeSnapshots,
			Namespace:     upd.Namespace,
			Name:          upd.Name,
		},
	}

	util.AddAnnotations(&pvc.ObjectMeta, annotations)
	util.AddLabels(&pvc.ObjectMeta, labels)

	p.Log.Infof("Returning from PVCBackupItemAction with %d additionalItems to backup", len(additionalItems))
	for _, ai := range additionalItems {
		p.Log.Debugf("%s: %s", ai.GroupResource.String(), ai.Name)
	}

	pvcMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pvc)
	if err != nil {
		return nil, nil, "", nil, errors.WithStack(err)
	}

	return &unstructured.Unstructured{Object: pvcMap}, additionalItems, "", nil, nil
}

func (p *PVCBackupItemAction) Name() string {
	return "PVCBackupItemAction"
}

func (p *PVCBackupItemAction) Progress(operationID string, backup *velerov1api.Backup) (velero.OperationProgress, error) {
	progress := velero.OperationProgress{}
	if operationID == "" {
		return progress, biav2.InvalidOperationIDError(operationID)
	}

	return progress, nil
}

func (p *PVCBackupItemAction) Cancel(operationID string, backup *velerov1api.Backup) error {
	return nil
}

func gcRetainedSnapshots(ctx context.Context, snapshotClient snapshotterClientSet.Interface, pvcID string, limit int, operationTimeout time.Duration, logger logrus.FieldLogger) error {
	labelSelector := util.SnapshotVolumeLabel + "=" + pvcID
	listItem, err := snapshotClient.SnapshotV1().VolumeSnapshotContents().List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return errors.Wrapf(err, "error to list snapshot contents with label %s", labelSelector)
	}

	retainedVSC := []*snapshotv1api.VolumeSnapshotContent{}
	for i := 0; i < len(listItem.Items); i++ {
		if _, exists := listItem.Items[i].Labels[velerov1api.DataMoverSnapshotRemoved]; !exists {
			retainedVSC = append(retainedVSC, &listItem.Items[i])
		}
	}

	if len(retainedVSC)+1 <= limit {
		logger.Infof("Don't need to gc retained snapshots, total %v, limit %v", len(retainedVSC), limit)
		return nil
	}

	logger.Infof("Need to gc retained snapshots, total %v, limit %v", len(retainedVSC), limit)

	sort.Slice(retainedVSC, func(i, j int) bool {
		return retainedVSC[i].CreationTimestamp.Before(&retainedVSC[j].CreationTimestamp)
	})

	for i := 0; i < len(retainedVSC)-limit+1; i++ {
		logger.Infof("GC retained snapshot %s from backup %s", retainedVSC[i].Name, retainedVSC[i].Labels[velerov1api.BackupNameLabel])

		err = util.SetVolumeSnapshotContentDeletionPolicy(retainedVSC[i].Name, snapshotClient.SnapshotV1())
		if err != nil {
			return errors.Wrapf(err, "error to set DeletionPolicy on volumesnapshotcontent %s", retainedVSC[i].Name)
		}

		err = util.EnsureDeleteVSC(ctx, snapshotClient, retainedVSC[i].Name, operationTimeout)
		if err != nil {
			return errors.Wrapf(err, "error to delete volumesnapshotcontent %s", retainedVSC[i].Name)
		}

		labels := retainedVSC[i].Labels
		labels[velerov1api.DataMoverSnapshotRemoved] = "true"
		sentinelVSC := snapshotv1api.VolumeSnapshotContent{
			ObjectMeta: metav1.ObjectMeta{
				Name:   retainedVSC[i].Name,
				Labels: labels,
			},
			Spec: snapshotv1api.VolumeSnapshotContentSpec{
				DeletionPolicy:    snapshotv1api.VolumeSnapshotContentRetain,
				Driver:            retainedVSC[i].Spec.Driver,
				VolumeSnapshotRef: retainedVSC[i].Spec.VolumeSnapshotRef,
				Source:            retainedVSC[i].Spec.Source,
			},
		}

		_, err = snapshotClient.SnapshotV1().VolumeSnapshotContents().Create(ctx, &sentinelVSC, metav1.CreateOptions{})
		if err != nil {
			logger.WithError(err).Warnf("error to create sentinel volumesnapshotcontent %s", sentinelVSC.Name)
		}
	}

	return nil
}
