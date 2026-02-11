/*
Copyright (c) 2025 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
*/

package controller

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	clnt "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	ckv1alpha1 "github.com/osac/osac-operator/api/v1alpha1"
	privatev1 "github.com/osac/osac-operator/internal/api/private/v1"
	sharedv1 "github.com/osac/osac-operator/internal/api/shared/v1"
)

// ComputeInstanceFeedbackReconciler sends updates to the fulfillment service.
type ComputeInstanceFeedbackReconciler struct {
	hubClient                clnt.Client
	computeInstancesClient   privatev1.ComputeInstancesClient
	computeInstanceNamespace string
}

// computeInstanceFeedbackReconcilerTask contains data that is used for the reconciliation of a specific compute instance, so there is less
// need to pass around as function parameters that and other related objects.
type computeInstanceFeedbackReconcilerTask struct {
	r      *ComputeInstanceFeedbackReconciler
	object *ckv1alpha1.ComputeInstance
	ci     *privatev1.ComputeInstance
}

// NewComputeInstanceFeedbackReconciler creates a reconciler that sends to the fulfillment service updates about compute instances.
func NewComputeInstanceFeedbackReconciler(hubClient clnt.Client, grpcConn *grpc.ClientConn, computeInstanceNamespace string) *ComputeInstanceFeedbackReconciler {
	return &ComputeInstanceFeedbackReconciler{
		hubClient:                hubClient,
		computeInstancesClient:   privatev1.NewComputeInstancesClient(grpcConn),
		computeInstanceNamespace: computeInstanceNamespace,
	}
}

// SetupWithManager adds the reconciler to the controller manager.
func (r *ComputeInstanceFeedbackReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("computeinstance-feedback").
		For(&ckv1alpha1.ComputeInstance{}, builder.WithPredicates(ComputeInstanceNamespacePredicate(r.computeInstanceNamespace))).
		Complete(r)
}

// Reconcile is the implementation of the reconciler interface.
func (r *ComputeInstanceFeedbackReconciler) Reconcile(ctx context.Context, request ctrl.Request) (result ctrl.Result, err error) {
	log := ctrllog.FromContext(ctx)

	// Step 1: Fetch the CR.
	object := &ckv1alpha1.ComputeInstance{}
	err = r.hubClient.Get(ctx, request.NamespacedName, object)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return //nolint:nakedret
		}
		// CR is gone. With the finalizer this shouldn't normally happen, but
		// handle gracefully (e.g. finalizer was removed externally).
		log.Info("CR not found, nothing to do")
		err = nil
		return //nolint:nakedret
	}

	// Step 2: Get the CI ID from labels. If missing, the object wasn't created
	// by the fulfillment service, so we ignore it.
	ciID, ok := object.Labels[cloudkitComputeInstanceIDLabel]
	if !ok {
		// If being deleted and somehow has our finalizer, remove it to unblock deletion.
		if !object.DeletionTimestamp.IsZero() && controllerutil.ContainsFinalizer(object, cloudkitComputeInstanceFeedbackFinalizer) {
			log.Info("CR without CI ID label is being deleted, removing feedback finalizer")
			if controllerutil.RemoveFinalizer(object, cloudkitComputeInstanceFeedbackFinalizer) {
				err = r.hubClient.Update(ctx, object)
			}
			return //nolint:nakedret
		}
		log.Info(
			"There is no label containing the compute instance identifier, will ignore it",
			"label", cloudkitComputeInstanceIDLabel,
		)
		return
	}

	// Step 3: If NOT being deleted, ensure our finalizer is present.
	if object.DeletionTimestamp.IsZero() {
		if controllerutil.AddFinalizer(object, cloudkitComputeInstanceFeedbackFinalizer) {
			if err = r.hubClient.Update(ctx, object); err != nil {
				return //nolint:nakedret
			}
		}
	}

	// Step 4: Sync state to the fulfillment service.
	ci, err := r.fetchComputeInstance(ctx, ciID)
	if err != nil {
		return
	}

	t := &computeInstanceFeedbackReconcilerTask{
		r:      r,
		object: object,
		ci:     clone(ci),
	}

	t.handleUpdate(ctx)

	err = r.saveComputeInstance(ctx, ci, t.ci)
	if err != nil {
		return
	}

	// Steps 5/6: If being deleted and our finalizer is present, check if we're
	// the last finalizer remaining.
	if !object.DeletionTimestamp.IsZero() && controllerutil.ContainsFinalizer(object, cloudkitComputeInstanceFeedbackFinalizer) {
		if len(object.GetFinalizers()) == 1 {
			// Step 5: We're the last finalizer. Remove our finalizer first to
			// allow garbage collection, then signal the fulfillment service.
			// This order ensures the K8s CR is gone before the fulfillment
			// controller checks — avoiding a race where it sees the CR still
			// exists and skips archival.
			log.Info(
				"Feedback finalizer is last remaining, removing finalizer and signaling",
				"ciID", ciID,
			)
			if controllerutil.RemoveFinalizer(object, cloudkitComputeInstanceFeedbackFinalizer) {
				err = r.hubClient.Update(ctx, object)
				if err != nil {
					return
				}
			}
			_, signalErr := r.computeInstancesClient.Signal(ctx, privatev1.ComputeInstancesSignalRequest_builder{
				Id: ciID,
			}.Build())
			if signalErr != nil {
				log.Error(
					signalErr,
					"Failed to signal fulfillment service, periodic sync will handle cleanup",
					"ciID", ciID,
				)
			}
		} else {
			// Step 6: Other finalizers still present. No action needed — when
			// another controller removes its finalizer, the Update event will
			// trigger a new reconcile.
			log.Info(
				"Other finalizers still present, waiting",
				"finalizers", object.GetFinalizers(),
			)
		}
	}

	return
}

func (r *ComputeInstanceFeedbackReconciler) fetchComputeInstance(ctx context.Context, id string) (vm *privatev1.ComputeInstance, err error) {
	response, err := r.computeInstancesClient.Get(ctx, privatev1.ComputeInstancesGetRequest_builder{
		Id: id,
	}.Build())
	if err != nil {
		return
	}
	vm = response.GetObject()
	if !vm.HasSpec() {
		vm.SetSpec(&privatev1.ComputeInstanceSpec{})
	}
	if !vm.HasStatus() {
		vm.SetStatus(&privatev1.ComputeInstanceStatus{})
	}
	return
}

func (r *ComputeInstanceFeedbackReconciler) saveComputeInstance(ctx context.Context, before, after *privatev1.ComputeInstance) error {
	log := ctrllog.FromContext(ctx)

	if !equal(after, before) {
		log.Info(
			"Updating compute instance",
			"before", before,
			"after", after,
		)
		_, err := r.computeInstancesClient.Update(ctx, privatev1.ComputeInstancesUpdateRequest_builder{
			Object: after,
		}.Build())
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *computeInstanceFeedbackReconcilerTask) handleUpdate(ctx context.Context) {
	t.syncConditions(ctx)
	t.syncPhase(ctx)
	t.syncIPAddress()
	t.syncLastRestartedAt()
}

func (t *computeInstanceFeedbackReconcilerTask) syncConditions(ctx context.Context) {
	t.syncProgressing(ctx)
	t.syncAvailable(ctx)
	t.syncRestartInProgress(ctx)
	t.syncRestartFailed(ctx)
}

// syncProgressing synchronizes the PROGRESSING VM condition from multiple CR conditions.
// If any of Progressing, or Accepted is true, then PROGRESSING is set to true.
func (t *computeInstanceFeedbackReconcilerTask) syncProgressing(ctx context.Context) {
	progressingCondition := t.object.GetStatusCondition(ckv1alpha1.ComputeInstanceConditionProgressing)
	acceptedCondition := t.object.GetStatusCondition(ckv1alpha1.ComputeInstanceConditionAccepted)

	var newStatus sharedv1.ConditionStatus
	var message string

	if t.object.IsStatusConditionUnknown(ckv1alpha1.ComputeInstanceConditionProgressing) && t.object.IsStatusConditionUnknown(ckv1alpha1.ComputeInstanceConditionAccepted) {
		newStatus = sharedv1.ConditionStatus_CONDITION_STATUS_UNSPECIFIED
	} else if t.object.IsStatusConditionTrue(ckv1alpha1.ComputeInstanceConditionProgressing) {
		newStatus = t.mapConditionStatus(progressingCondition.Status)
		message = progressingCondition.Message
	} else if t.object.IsStatusConditionTrue(ckv1alpha1.ComputeInstanceConditionAccepted) {
		newStatus = t.mapConditionStatus(acceptedCondition.Status)
		message = acceptedCondition.Message
	} else {
		newStatus = sharedv1.ConditionStatus_CONDITION_STATUS_FALSE
	}

	vmCondition := t.findComputeInstanceCondition(privatev1.ComputeInstanceConditionType_COMPUTE_INSTANCE_CONDITION_TYPE_PROGRESSING)
	oldStatus := vmCondition.GetStatus()

	vmCondition.SetStatus(newStatus)
	vmCondition.SetMessage(message)
	if newStatus != oldStatus {
		vmCondition.SetLastTransitionTime(timestamppb.Now())
	}
}

// syncAvailable synchronizes the AVAILABLE VM condition from the Available CR condition.
func (t *computeInstanceFeedbackReconcilerTask) syncAvailable(ctx context.Context) {
	crCondition := t.object.GetStatusCondition(ckv1alpha1.ComputeInstanceConditionAvailable)
	if crCondition == nil {
		return
	}
	t.syncVMConditionFromCR(privatev1.ComputeInstanceConditionType_COMPUTE_INSTANCE_CONDITION_TYPE_AVAILABLE, crCondition)
}

// syncRestartInProgress synchronizes the RESTART_IN_PROGRESS VM condition from the RestartInProgress CR condition.
func (t *computeInstanceFeedbackReconcilerTask) syncRestartInProgress(ctx context.Context) {
	crCondition := t.object.GetStatusCondition(ckv1alpha1.ComputeInstanceConditionRestartInProgress)
	if crCondition == nil {
		return
	}
	t.syncVMConditionFromCR(privatev1.ComputeInstanceConditionType_COMPUTE_INSTANCE_CONDITION_TYPE_RESTART_IN_PROGRESS, crCondition)
}

// syncRestartFailed synchronizes the RESTART_FAILED VM condition from the RestartFailed CR condition.
func (t *computeInstanceFeedbackReconcilerTask) syncRestartFailed(ctx context.Context) {
	crCondition := t.object.GetStatusCondition(ckv1alpha1.ComputeInstanceConditionRestartFailed)
	if crCondition == nil {
		return
	}
	t.syncVMConditionFromCR(privatev1.ComputeInstanceConditionType_COMPUTE_INSTANCE_CONDITION_TYPE_RESTART_FAILED, crCondition)
}

// syncVMConditionFromCR synchronizes a VM condition from a CR condition.
func (t *computeInstanceFeedbackReconcilerTask) syncVMConditionFromCR(vmConditionType privatev1.ComputeInstanceConditionType, crCondition *metav1.Condition) {
	vmCondition := t.findComputeInstanceCondition(vmConditionType)
	oldStatus := vmCondition.GetStatus()
	newStatus := t.mapConditionStatus(crCondition.Status)
	vmCondition.SetStatus(newStatus)
	vmCondition.SetMessage(crCondition.Message)
	if newStatus != oldStatus {
		vmCondition.SetLastTransitionTime(timestamppb.Now())
	}
}

func (t *computeInstanceFeedbackReconcilerTask) mapConditionStatus(status metav1.ConditionStatus) sharedv1.ConditionStatus {
	switch status {
	case metav1.ConditionFalse:
		return sharedv1.ConditionStatus_CONDITION_STATUS_FALSE
	case metav1.ConditionTrue:
		return sharedv1.ConditionStatus_CONDITION_STATUS_TRUE
	default:
		return sharedv1.ConditionStatus_CONDITION_STATUS_UNSPECIFIED
	}
}

func (t *computeInstanceFeedbackReconcilerTask) syncPhase(ctx context.Context) {
	switch t.object.Status.Phase {
	case ckv1alpha1.ComputeInstancePhaseStarting:
		t.syncPhaseStarting()
	case ckv1alpha1.ComputeInstancePhaseFailed:
		t.syncPhaseFailed()
	case ckv1alpha1.ComputeInstancePhaseRunning:
		t.syncPhaseRunning()
	case ckv1alpha1.ComputeInstancePhaseDeleting:
		t.syncPhaseDeleting()
	default:
		log := ctrllog.FromContext(ctx)
		log.Info(
			"Unknown phase, will ignore it",
			"phase", t.object.Status.Phase,
		)
	}
}

func (t *computeInstanceFeedbackReconcilerTask) syncPhaseStarting() {
	t.ci.GetStatus().SetState(privatev1.ComputeInstanceState_COMPUTE_INSTANCE_STATE_STARTING)
}

func (t *computeInstanceFeedbackReconcilerTask) syncPhaseFailed() {
	t.ci.GetStatus().SetState(privatev1.ComputeInstanceState_COMPUTE_INSTANCE_STATE_FAILED)
}

func (t *computeInstanceFeedbackReconcilerTask) syncPhaseRunning() {
	ciStatus := t.ci.GetStatus()
	ciStatus.SetState(privatev1.ComputeInstanceState_COMPUTE_INSTANCE_STATE_RUNNING)
}

func (t *computeInstanceFeedbackReconcilerTask) syncPhaseDeleting() {
	t.ci.GetStatus().SetState(privatev1.ComputeInstanceState_COMPUTE_INSTANCE_STATE_DELETING)
}

func (t *computeInstanceFeedbackReconcilerTask) findComputeInstanceCondition(kind privatev1.ComputeInstanceConditionType) *privatev1.ComputeInstanceCondition {
	var condition *privatev1.ComputeInstanceCondition
	for _, current := range t.ci.Status.Conditions {
		if current.Type == kind {
			condition = current
			break
		}
	}
	if condition == nil {
		condition = &privatev1.ComputeInstanceCondition{
			Type:   kind,
			Status: sharedv1.ConditionStatus_CONDITION_STATUS_FALSE,
		}
		t.ci.Status.Conditions = append(t.ci.Status.Conditions, condition)
	}
	return condition
}

func (t *computeInstanceFeedbackReconcilerTask) syncIPAddress() {
	ipAddress, ok := t.object.Annotations[cloudkitVirualMachineFloatingIPAddressAnnotation]
	if ok && ipAddress != "" {
		t.ci.GetStatus().SetIpAddress(ipAddress)
	}
}

func (t *computeInstanceFeedbackReconcilerTask) syncLastRestartedAt() {
	if t.object.Status.LastRestartedAt != nil {
		t.ci.GetStatus().SetLastRestartedAt(timestamppb.New(t.object.Status.LastRestartedAt.Time))
	}
}
