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

package util

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	jsonpatch "github.com/evanphx/json-patch"
	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	snapshotter "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned/typed/volumesnapshot/v1"
	corev1api "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/clientcmd"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	veleroClientSet "github.com/vmware-tanzu/velero/pkg/generated/clientset/versioned"
	"github.com/vmware-tanzu/velero/pkg/label"
	"github.com/vmware-tanzu/velero/pkg/podvolume"
	"github.com/vmware-tanzu/velero/pkg/util/boolptr"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

const (
	kubeAnnBindCompleted     = "pv.kubernetes.io/bind-completed"
	kubeAnnBoundByController = "pv.kubernetes.io/bound-by-controller"
)

type DataMoveCompletionStatus string

const (
	DataMoveCompleted DataMoveCompletionStatus = "Completed"
	DataMoveFailed    DataMoveCompletionStatus = "Failed"
)

func GetPVForPVC(pvc *corev1api.PersistentVolumeClaim, corev1 corev1client.PersistentVolumesGetter) (*corev1api.PersistentVolume, error) {
	if pvc.Spec.VolumeName == "" {
		return nil, errors.Errorf("PVC %s/%s has no volume backing this claim", pvc.Namespace, pvc.Name)
	}
	if pvc.Status.Phase != corev1api.ClaimBound {
		// TODO: confirm if this PVC should be snapshotted if it has no PV bound
		return nil, errors.Errorf("PVC %s/%s is in phase %v and is not bound to a volume", pvc.Namespace, pvc.Name, pvc.Status.Phase)
	}
	pvName := pvc.Spec.VolumeName
	pv, err := corev1.PersistentVolumes().Get(context.TODO(), pvName, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get PV %s for PVC %s/%s", pvName, pvc.Namespace, pvc.Name)
	}
	return pv, nil
}

func GetPodsUsingPVC(pvcNamespace, pvcName string, corev1 corev1client.PodsGetter) ([]corev1api.Pod, error) {
	podsUsingPVC := []corev1api.Pod{}
	podList, err := corev1.Pods(pvcNamespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	for _, p := range podList.Items {
		for _, v := range p.Spec.Volumes {
			if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == pvcName {
				podsUsingPVC = append(podsUsingPVC, p)
			}
		}
	}

	return podsUsingPVC, nil
}

func GetPodVolumeNameForPVC(pod corev1api.Pod, pvcName string) (string, error) {
	for _, v := range pod.Spec.Volumes {
		if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == pvcName {
			return v.Name, nil
		}
	}
	return "", errors.Errorf("Pod %s/%s does not use PVC %s/%s", pod.Namespace, pod.Name, pod.Namespace, pvcName)
}

func Contains(slice []string, key string) bool {
	for _, i := range slice {
		if i == key {
			return true
		}
	}
	return false
}

func IsPVCDefaultToFSBackup(pvcNamespace, pvcName string, podClient corev1client.PodsGetter, defaultVolumesToFsBackup bool) (bool, error) {
	pods, err := GetPodsUsingPVC(pvcNamespace, pvcName, podClient)
	if err != nil {
		return false, errors.WithStack(err)
	}

	for _, p := range pods {
		vols := podvolume.GetVolumesByPod(&p, defaultVolumesToFsBackup)
		if len(vols) > 0 {
			volName, err := GetPodVolumeNameForPVC(p, pvcName)
			if err != nil {
				return false, err
			}
			if Contains(vols, volName) {
				return true, nil
			}
		}
	}

	return false, nil
}

// GetVolumeSnapshotClassForStorageClass returns a VolumeSnapshotClass for the supplied volume provisioner/ driver name.
func GetVolumeSnapshotClassForStorageClass(provisioner string, snapshotClient snapshotter.SnapshotV1Interface) (*snapshotv1api.VolumeSnapshotClass, error) {
	snapshotClasses, err := snapshotClient.VolumeSnapshotClasses().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "error listing volumesnapshot classes")
	}
	// We pick the volumesnapshotclass that matches the CSI driver name and has a 'velero.io/csi-volumesnapshot-class'
	// label. This allows multiple VolumesnapshotClasses for the same driver with different values for the
	// other fields in the spec.
	// https://github.com/kubernetes-csi/external-snapshotter/blob/release-4.2/client/config/crd/snapshot.storage.k8s.io_volumesnapshotclasses.yaml
	for _, sc := range snapshotClasses.Items {
		_, hasLabelSelector := sc.Labels[VolumeSnapshotClassSelectorLabel]
		if sc.Driver == provisioner && hasLabelSelector {
			return &sc, nil
		}
	}
	return nil, errors.Errorf("failed to get volumesnapshotclass for provisioner %s, ensure that the desired volumesnapshot class has the %s label", provisioner, VolumeSnapshotClassSelectorLabel)
}

// GetVolumeSnapshotContentForVolumeSnapshot returns the volumesnapshotcontent object associated with the volumesnapshot
func GetVolumeSnapshotContentForVolumeSnapshot(volSnap *snapshotv1api.VolumeSnapshot, snapshotClient snapshotter.SnapshotV1Interface, log logrus.FieldLogger, shouldWait bool) (*snapshotv1api.VolumeSnapshotContent, error) {
	if !shouldWait {
		if volSnap.Status == nil || volSnap.Status.BoundVolumeSnapshotContentName == nil {
			// volumesnapshot hasn't been reconciled and we're not waiting for it.
			return nil, nil
		}
		vsc, err := snapshotClient.VolumeSnapshotContents().Get(context.TODO(), *volSnap.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		if err != nil {
			return nil, errors.Wrap(err, "error getting volume snapshot content from API")
		}
		return vsc, nil
	}

	// We'll wait 10m for the VSC to be reconciled polling every 5s
	// TODO: make this timeout configurable.
	timeout := 10 * time.Minute
	interval := 5 * time.Second
	var snapshotContent *snapshotv1api.VolumeSnapshotContent

	err := wait.PollImmediate(interval, timeout, func() (bool, error) {
		vs, err := snapshotClient.VolumeSnapshots(volSnap.Namespace).Get(context.TODO(), volSnap.Name, metav1.GetOptions{})
		if err != nil {
			return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshot %s/%s", volSnap.Namespace, volSnap.Name))
		}

		if vs.Status == nil || vs.Status.BoundVolumeSnapshotContentName == nil {
			log.Infof("Waiting for CSI driver to reconcile volumesnapshot %s/%s. Retrying in %ds", volSnap.Namespace, volSnap.Name, interval/time.Second)
			return false, nil
		}

		snapshotContent, err = snapshotClient.VolumeSnapshotContents().Get(context.TODO(), *vs.Status.BoundVolumeSnapshotContentName, metav1.GetOptions{})
		if err != nil {
			return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshotcontent %s for volumesnapshot %s/%s", *vs.Status.BoundVolumeSnapshotContentName, vs.Namespace, vs.Name))
		}

		// we need to wait for the VolumeSnaphotContent to have a snapshot handle because during restore,
		// we'll use that snapshot handle as the source for the VolumeSnapshotContent so it's statically
		// bound to the existing snapshot.
		if snapshotContent.Status == nil || snapshotContent.Status.SnapshotHandle == nil {
			log.Infof("Waiting for volumesnapshotcontents %s to have snapshot handle. Retrying in %ds", snapshotContent.Name, interval/time.Second)
			if snapshotContent.Status != nil && snapshotContent.Status.Error != nil {
				log.Warnf("Volumesnapshotcontent %s has error: %v", snapshotContent.Name, snapshotContent.Status.Error.Message)
			}
			return false, nil
		}

		return true, nil
	})

	if err != nil {
		if err == wait.ErrWaitTimeout {
			if snapshotContent.Status != nil && snapshotContent.Status.Error != nil {
				log.Errorf("Timed out awaiting reconciliation of volumesnapshot, Volumesnapshotcontent %s has error: %v", snapshotContent.Name, snapshotContent.Status.Error.Message)
			} else {
				log.Errorf("Timed out awaiting reconciliation of volumesnapshot %s/%s", volSnap.Namespace, volSnap.Name)
			}
		}
		return nil, err
	}

	return snapshotContent, nil
}

func GetClients() (*kubernetes.Clientset, *snapshotterClientSet.Clientset, error) {
	client, snapshotterClient, _, err := GetFullClients()

	return client, snapshotterClient, err
}

func GetFullClients() (*kubernetes.Clientset, *snapshotterClientSet.Clientset, *veleroClientSet.Clientset, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides)
	clientConfig, err := kubeConfig.ClientConfig()
	if err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}

	client, err := kubernetes.NewForConfig(clientConfig)
	if err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}

	snapshotterClient, err := snapshotterClientSet.NewForConfig(clientConfig)
	if err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}

	veleroClient, err := veleroClientSet.NewForConfig(clientConfig)
	if err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}

	return client, snapshotterClient, veleroClient, nil
}

// IsVolumeSnapshotClassHasListerSecret returns whether a volumesnapshotclass has a snapshotlister secret
func IsVolumeSnapshotClassHasListerSecret(vc *snapshotv1api.VolumeSnapshotClass) bool {
	// https://github.com/kubernetes-csi/external-snapshotter/blob/master/pkg/utils/util.go#L59-L60
	// There is no release w/ these constants exported. Using the strings for now.
	_, nameExists := vc.Annotations[PrefixedSnapshotterListSecretNameKey]
	_, nsExists := vc.Annotations[PrefixedSnapshotterListSecretNamespaceKey]
	return nameExists && nsExists
}

// IsVolumeSnapshotContentHasDeleteSecret returns whether a volumesnapshotcontent has a deletesnapshot secret
func IsVolumeSnapshotContentHasDeleteSecret(vsc *snapshotv1api.VolumeSnapshotContent) bool {
	// https://github.com/kubernetes-csi/external-snapshotter/blob/master/pkg/utils/util.go#L56-L57
	// use exported constants in the next release
	_, nameExists := vsc.Annotations[PrefixedSnapshotterSecretNameKey]
	_, nsExists := vsc.Annotations[PrefixedSnapshotterSecretNamespaceKey]
	return nameExists && nsExists
}

// IsVolumeSnapshotHasVSCDeleteSecret returns whether a volumesnapshot should set the deletesnapshot secret
// for the static volumesnapshotcontent that is created on restore
func IsVolumeSnapshotHasVSCDeleteSecret(vs *snapshotv1api.VolumeSnapshot) bool {
	_, nameExists := vs.Annotations[CSIDeleteSnapshotSecretName]
	_, nsExists := vs.Annotations[CSIDeleteSnapshotSecretNamespace]
	return nameExists && nsExists
}

// AddAnnotations adds the supplied key-values to the annotations on the object
func AddAnnotations(o *metav1.ObjectMeta, vals map[string]string) {
	if o.Annotations == nil {
		o.Annotations = make(map[string]string)
	}
	for k, v := range vals {
		o.Annotations[k] = v
	}
}

// AddLabels adds the supplied key-values to the labels on the object
func AddLabels(o *metav1.ObjectMeta, vals map[string]string) {
	if o.Labels == nil {
		o.Labels = make(map[string]string)
	}
	for k, v := range vals {
		o.Labels[k] = label.GetValidName(v)
	}
}

// IsVolumeSnapshotExists returns whether a specific volumesnapshot object exists.
func IsVolumeSnapshotExists(volSnap *snapshotv1api.VolumeSnapshot, snapshotClient snapshotter.SnapshotV1Interface) bool {
	exists := false
	if volSnap != nil {
		vs, err := snapshotClient.VolumeSnapshots(volSnap.Namespace).Get(context.TODO(), volSnap.Name, metav1.GetOptions{})
		if err == nil && vs != nil {
			exists = true
		}
	}

	return exists
}

func SetVolumeSnapshotContentDeletionPolicy(vscName string, csiClient snapshotter.SnapshotV1Interface) error {
	pb := []byte(`{"spec":{"deletionPolicy":"Delete"}}`)
	_, err := csiClient.VolumeSnapshotContents().Patch(context.TODO(), vscName, types.MergePatchType, pb, metav1.PatchOptions{})

	return err
}

func HasBackupLabel(o *metav1.ObjectMeta, backupName string) bool {
	if o.Labels == nil || len(strings.TrimSpace(backupName)) == 0 {
		return false
	}
	return o.Labels[velerov1api.BackupNameLabel] == label.GetValidName(backupName)
}

func IsMovingVolumeSnapshot() bool {
	return true
}

func GetVolumeSnapshotWaitTimeout() time.Duration {
	defaultCSISnapshotTimeout := 10 * time.Minute
	return defaultCSISnapshotTimeout
}

func GetDataMovementWaitTimeout() time.Duration {
	defaultDataMovementTimeout := 10 * time.Minute
	return defaultDataMovementTimeout
}

func GetBindWaitTimeout() time.Duration {
	defaultBindTimeout := 1 * time.Minute
	return defaultBindTimeout
}

func GetUploaderType() string {
	return "kopia"
}

func WaitVolumeSnapshotReady(ctx context.Context, snapshotClient *snapshotterClientSet.Clientset,
	volumeSnapshot *snapshotv1api.VolumeSnapshot, timeout time.Duration, Log logrus.FieldLogger) (*snapshotv1api.VolumeSnapshot, error) {
	eg, _ := errgroup.WithContext(ctx)
	interval := 5 * time.Second

	var updated *snapshotv1api.VolumeSnapshot
	eg.Go(func() error {
		err := wait.PollImmediate(interval, timeout, func() (bool, error) {
			tmpVS, err := snapshotClient.SnapshotV1().VolumeSnapshots(volumeSnapshot.Namespace).Get(ctx, volumeSnapshot.Name, metav1.GetOptions{})
			if err != nil {
				return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshot %s/%s", volumeSnapshot.Namespace, volumeSnapshot.Name))
			}
			if tmpVS.Status == nil || tmpVS.Status.BoundVolumeSnapshotContentName == nil || !boolptr.IsSetToTrue(tmpVS.Status.ReadyToUse) || tmpVS.Status.RestoreSize == nil {
				Log.Infof("Waiting for CSI driver to reconcile volumesnapshot %s/%s. Retrying in %ds", volumeSnapshot.Namespace, volumeSnapshot.Name, interval/time.Second)
				return false, nil
			}

			updated = tmpVS
			Log.Debugf("VolumeSnapshot %s/%s turned into ReadyToUse.", volumeSnapshot.Namespace, volumeSnapshot.Name)

			return true, nil
		})

		return err
	})

	err := eg.Wait()

	return updated, err
}

func DeletePVCIfAny(ctx context.Context, kubeClient *kubernetes.Clientset, pvc *corev1api.PersistentVolumeClaim, log logrus.FieldLogger) {
	if pvc != nil {
		err := kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(ctx, pvc.Name, *&metav1.DeleteOptions{})
		if err != nil {
			log.WithError(err).Errorf("Failed to delete pvc %s", pvc.Name)
		}
	}
}

func DeleteVolumeSnapshotIfAny(ctx context.Context, snapshotClient *snapshotterClientSet.Clientset, volumeSnapshot *snapshotv1api.VolumeSnapshot, log logrus.FieldLogger) {
	if volumeSnapshot != nil {
		err := snapshotClient.SnapshotV1().VolumeSnapshots(volumeSnapshot.Namespace).Delete(ctx, volumeSnapshot.Name, *&metav1.DeleteOptions{})
		if err != nil {
			log.WithError(err).Errorf("Failed to delete volume snapshot %s", volumeSnapshot.Name)
		}
	}
}

func GetVolumeModeFromBackup(backup *velerov1api.Backup) corev1api.PersistentVolumeMode {
	return corev1api.PersistentVolumeFilesystem
}

func GetVolumeModeFromRestore(backup *velerov1api.Restore) corev1api.PersistentVolumeMode {
	return corev1api.PersistentVolumeFilesystem
}

func WaitPVCBound(ctx context.Context, pvcGetter corev1client.PersistentVolumeClaimsGetter,
	pvGetter corev1client.PersistentVolumesGetter, pvc *corev1api.PersistentVolumeClaim,
	timeout time.Duration) (*corev1api.PersistentVolumeClaim, *corev1api.PersistentVolume, error) {
	eg, _ := errgroup.WithContext(ctx)
	interval := 5 * time.Second

	var updated *corev1api.PersistentVolumeClaim
	eg.Go(func() error {
		err := wait.PollImmediate(interval, timeout, func() (bool, error) {
			tmpPVC, err := pvcGetter.PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
			if err != nil {
				return false, errors.Wrapf(err, fmt.Sprintf("failed to get pvc %s/%s", pvc.Namespace, pvc.Name))
			}

			if tmpPVC.Spec.VolumeName == "" {
				return false, nil
			}

			updated = tmpPVC

			return true, nil
		})

		return err
	})

	err := eg.Wait()
	if err != nil {
		return nil, nil, err
	}

	pv, err := pvGetter.PersistentVolumes().Get(ctx, updated.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, err
	}

	return updated, pv, err
}

func WaitPVBound(ctx context.Context, pvGetter corev1client.PersistentVolumesGetter,
	pv *corev1api.PersistentVolume, timeout time.Duration) (*corev1api.PersistentVolume, error) {
	eg, _ := errgroup.WithContext(ctx)
	interval := 5 * time.Second

	var updated *corev1api.PersistentVolume
	eg.Go(func() error {
		err := wait.PollImmediate(interval, timeout, func() (bool, error) {
			tmpPV, err := pvGetter.PersistentVolumes().Get(ctx, pv.Name, metav1.GetOptions{})
			if err != nil {
				return false, errors.Wrapf(err, fmt.Sprintf("failed to get pv %s", pv.Name))
			}

			if tmpPV.Spec.ClaimRef == nil {
				return false, nil
			}

			updated = tmpPV

			return true, nil
		})

		return err
	})

	err := eg.Wait()
	if err != nil {
		return nil, err
	}

	return updated, err
}

func RebindPV(ctx context.Context, pvGetter corev1client.PersistentVolumesGetter,
	pv *corev1api.PersistentVolume, pvc *corev1api.PersistentVolumeClaim) error {

	updated := pv.DeepCopy()
	updated.Name = updated.Name + "-complete"
	delete(updated.Annotations, kubeAnnBindCompleted)
	delete(updated.Annotations, kubeAnnBoundByController)
	pv.Spec.ClaimRef = nil

	var patchData []byte
	patchData, err := json.Marshal(updated)
	if err != nil {
		return err
	}

	_, err = pvGetter.PersistentVolumes().Patch(ctx, pv.Name, types.MergePatchType, patchData, metav1.PatchOptions{})
	if err != nil {
		return err
	}

	return nil
}

func WaitPVCBindLost(ctx context.Context, pvcGetter corev1client.PersistentVolumeClaimsGetter,
	pvc *corev1api.PersistentVolumeClaim, timeout time.Duration) (*corev1api.PersistentVolumeClaim, error) {
	eg, _ := errgroup.WithContext(ctx)
	interval := 5 * time.Second

	var updated *corev1api.PersistentVolumeClaim
	eg.Go(func() error {
		err := wait.PollImmediate(interval, timeout, func() (bool, error) {
			tmpPVC, err := pvcGetter.PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
			if err != nil {
				return false, errors.Wrapf(err, fmt.Sprintf("failed to get pvc %s/%s", pvc.Namespace, pvc.Name))
			}

			if tmpPVC.Status.Phase != corev1api.ClaimLost {
				return false, nil
			}

			updated = tmpPVC

			return true, nil
		})

		return err
	})

	err := eg.Wait()
	if err != nil {
		return nil, err
	}

	return updated, err
}

func SetPVCStorageResourceRequest(pvc *corev1api.PersistentVolumeClaim, restoreSize resource.Quantity, log logrus.FieldLogger) {
	{
		if pvc.Spec.Resources.Requests == nil {
			pvc.Spec.Resources.Requests = corev1api.ResourceList{}
		}

		storageReq, exists := pvc.Spec.Resources.Requests[corev1api.ResourceStorage]
		if !exists || storageReq.Cmp(restoreSize) < 0 {
			pvc.Spec.Resources.Requests[corev1api.ResourceStorage] = restoreSize
			rs := pvc.Spec.Resources.Requests[corev1api.ResourceStorage]
			log.Infof("Resetting storage requests for PVC %s/%s to %s", pvc.Namespace, pvc.Name, rs.String())
		}
	}
}

func ResetPVCSpec(pvc *corev1api.PersistentVolumeClaim, vsName string) {
	// Restore operation for the PVC will use the volumesnapshot as the data source.
	// So clear out the volume name, which is a ref to the PV
	pvc.Spec.VolumeName = ""
	dataSourceRef := &corev1api.TypedLocalObjectReference{
		APIGroup: &snapshotv1api.SchemeGroupVersion.Group,
		Kind:     "VolumeSnapshot",
		Name:     vsName,
	}
	pvc.Spec.DataSource = dataSourceRef
	pvc.Spec.DataSourceRef = dataSourceRef
}

func RetainVSCIfAny(ctx context.Context, snapshotClient *snapshotterClientSet.Clientset, vsc *snapshotv1api.VolumeSnapshotContent) (bool, error) {
	if vsc.Spec.DeletionPolicy == snapshotv1api.VolumeSnapshotContentRetain {
		return false, nil
	}
	origBytes, err := json.Marshal(vsc)
	if err != nil {
		return false, errors.Wrap(err, "error marshalling original VSC")
	}

	updated := vsc.DeepCopy()
	updated.Spec.DeletionPolicy = snapshotv1api.VolumeSnapshotContentRetain

	updatedBytes, err := json.Marshal(updated)
	if err != nil {
		return false, errors.Wrap(err, "error marshalling updated VSC")
	}

	patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
	if err != nil {
		return false, errors.Wrap(err, "error creating json merge patch for VSC")
	}

	_, err = snapshotClient.SnapshotV1().VolumeSnapshotContents().Patch(ctx, vsc.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return false, err
	}

	return true, nil
}

func SetPVReclaimPolicy(ctx context.Context, kubeClient *kubernetes.Clientset,
	pv *corev1api.PersistentVolume, policy corev1api.PersistentVolumeReclaimPolicy) (corev1api.PersistentVolumeReclaimPolicy, error) {
	curPolicy := pv.Spec.PersistentVolumeReclaimPolicy
	if curPolicy == policy {
		return curPolicy, nil
	}

	origBytes, err := json.Marshal(pv)
	if err != nil {
		return curPolicy, errors.Wrap(err, "error marshalling original PV")
	}

	updated := pv.DeepCopy()
	updated.Spec.PersistentVolumeReclaimPolicy = policy

	updatedBytes, err := json.Marshal(updated)
	if err != nil {
		return curPolicy, errors.Wrap(err, "error marshalling updated PV")
	}

	patchBytes, err := jsonpatch.CreateMergePatch(origBytes, updatedBytes)
	if err != nil {
		return curPolicy, errors.Wrap(err, "error creating json merge patch for PV")
	}

	_, err = kubeClient.CoreV1().PersistentVolumes().Patch(ctx, pv.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return curPolicy, err
	}

	return curPolicy, nil
}

func EnsureDeleteVS(ctx context.Context, snapshotClient *snapshotterClientSet.Clientset,
	vs *snapshotv1api.VolumeSnapshot, timeout time.Duration, log logrus.FieldLogger) error {
	if vs == nil {
		return nil
	}

	err := snapshotClient.SnapshotV1().VolumeSnapshots(vs.Namespace).Delete(ctx, vs.Name, metav1.DeleteOptions{})
	if err != nil {
		return errors.Wrap(err, "error to delete volume snapshot")
	}

	interval := 1 * time.Second
	err = wait.PollImmediate(interval, timeout, func() (bool, error) {
		_, err := snapshotClient.SnapshotV1().VolumeSnapshots(vs.Namespace).Get(ctx, vs.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			return false, errors.Wrapf(err, fmt.Sprintf("failed to get VolumeSnapshot %s", vs.Name))
		}

		return false, nil
	})

	if err != nil {
		return errors.Wrapf(err, "fail to retrieve VolumeSnapshot info for %s", vs.Name)
	}

	return nil
}

func EnsureDeleteVSC(ctx context.Context, snapshotClient *snapshotterClientSet.Clientset,
	vsc *snapshotv1api.VolumeSnapshotContent, timeout time.Duration, log logrus.FieldLogger) error {
	if vsc == nil {
		return nil
	}

	err := snapshotClient.SnapshotV1().VolumeSnapshotContents().Delete(ctx, vsc.Name, metav1.DeleteOptions{})
	if err != nil {
		return errors.Wrap(err, "error to delete volume snapshotContent")
	}

	interval := 1 * time.Second
	err = wait.PollImmediate(interval, timeout, func() (bool, error) {
		_, err := snapshotClient.SnapshotV1().VolumeSnapshotContents().Get(ctx, vsc.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			return false, errors.Wrapf(err, fmt.Sprintf("failed to get VolumeSnapshotContent %s", vsc.Name))
		}

		return false, nil
	})

	if err != nil {
		return errors.Wrapf(err, "fail to retrieve VolumeSnapshotContent info for %s", vsc.Name)
	}

	return nil
}

func EnsureDeletePVC(ctx context.Context, kubeClient *kubernetes.Clientset,
	pvc *corev1api.PersistentVolumeClaim, timeout time.Duration, log logrus.FieldLogger) error {
	if pvc == nil {
		return nil
	}

	err := kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Delete(ctx, pvc.Name, metav1.DeleteOptions{})
	if err != nil {
		return errors.Wrap(err, "error to delete pvc")
	}

	interval := 1 * time.Second
	err = wait.PollImmediate(interval, timeout, func() (bool, error) {
		_, err := kubeClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return true, nil
			}

			return false, errors.Wrapf(err, fmt.Sprintf("failed to get pvc %s", pvc.Name))
		}

		return false, nil
	})

	if err != nil {
		return errors.Wrapf(err, "fail to retrieve pvc info for %s", pvc.Name)
	}

	return nil
}
