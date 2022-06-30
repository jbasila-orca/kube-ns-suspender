package engine

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	kedaclientset "github.com/kedacore/keda/v2/pkg/generated/clientset/versioned"
)

// checkRunningDeploymentsConformity verifies that all deployments within the namespace are
// currently running
func checkRunningScaledobjectsConformity(ctx context.Context, l zerolog.Logger, scaledobjects []kedav1alpha1.ScaledObject, cs *kedaclientset.Clientset, ns, prefix string) (bool, error) {
	hasBeenPatched := false
	for _, so := range scaledobjects {
		// Check if the deployment is annotated and deployment name
		origName := so.Annotations[prefix+originalScaleTargetRefName]
		if origName != "" {
			patchedName := fmt.Sprintf("%s-suspend", origName)
			if so.Spec.ScaleTargetRef.Name != patchedName {
				continue
			}
			l.Info().Str("scaledobject", so.Name).Msgf("changing scaleTargetRef name from %s to %s", so.Spec.ScaleTargetRef.Name, origName)
			// patch the deployment
			if err := patchScaledobjects(ctx, cs, ns, so.Name, prefix, origName); err != nil {
				return hasBeenPatched, err
			}
			hasBeenPatched = true
		}
	}
	return hasBeenPatched, nil
}

func checkSuspendedScaledobejctsConformity(ctx context.Context, l zerolog.Logger, scaledobjects []kedav1alpha1.ScaledObject, cs *kedaclientset.Clientset, ns, prefix string) error {
	if scaledobjects == nil {
		return nil
	}

	for _, so := range scaledobjects {
		origName := so.Annotations[prefix+originalScaleTargetRefName]
		if origName == "" || so.Spec.ScaleTargetRef.Name == origName {
			// TODO: what about fixing the annotation original Replicas here ?
			patchedName := fmt.Sprintf("%s-suspend", so.Spec.ScaleTargetRef.Name)
			l.Info().Str("scaledobjects", so.Name).Msgf("changing scaleTargetRef name from %s to %s", origName, patchedName)
			// patch the deployment
			if err := patchScaledobjects(ctx, cs, ns, so.Name, prefix, patchedName); err != nil {
				return err
			}
		}
	}
	return nil
}

// patchScaledobjects updates the minmum and maximum deployments
func patchScaledobjects(ctx context.Context, cs *kedaclientset.Clientset, ns, so, prefix string, patchedName string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		result, err := cs.KedaV1alpha1().ScaledObjects(ns).Get(ctx, so, metav1.GetOptions{})
		if err != nil {
			return err
		}
		// If there's no annotation, set it up
		if result.Annotations[prefix+originalScaleTargetRefName] == "" {
			result.Annotations[prefix+originalScaleTargetRefName] = result.Spec.ScaleTargetRef.Name
		}
		result.Spec.ScaleTargetRef.Name = patchedName
		_, err = cs.KedaV1alpha1().ScaledObjects(ns).Update(ctx, result, v1.UpdateOptions{})
		return err
	})
}
