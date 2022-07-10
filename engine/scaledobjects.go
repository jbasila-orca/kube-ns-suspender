package engine

import (
	"context"

	
	"github.com/rs/zerolog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	kedaclientset "github.com/kedacore/keda/v2/pkg/generated/clientset/versioned"
)


func checkRunningScaledobjectsConformity(ctx context.Context, l zerolog.Logger, scaledobjects []kedav1alpha1.ScaledObject, cs *kedaclientset.Clientset, ns, prefix string) (bool, error) {
	hasBeenPatched := false
	for _, so := range scaledobjects {
		// Check if the ScaledObject is annotated
		if _, exits := so.Annotations[kedaAutoscalingPaused]; exits {
			l.Info().Str("scaledobjects", so.Name).Msgf("Removing annotations %s from the ScaledObject", kedaAutoscalingPaused)
			origName := so.Annotations[prefix+originalScaleTargetRefName]
			if err := patchScaledobjects(ctx, cs, ns, so.Name, prefix, origName, false); err != nil {
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
		if _, exists := so.Annotations[kedaAutoscalingPaused]; exists {
			continue
		} else {
			l.Info().Str("scaledobject", so.Name).Msgf("Patching the ScaledObject with %s", kedaAutoscalingPaused)
			if err := patchScaledobjects(ctx, cs, ns, so.Name, prefix, "", true); err != nil {
				return err
			}
		}
	}

	return nil
}

// patchScaledobjects updates the minmum and maximum deployments
func patchScaledobjects(ctx context.Context, cs *kedaclientset.Clientset, ns, so, prefix string, patchedName string, suspend bool) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		result, err := cs.KedaV1alpha1().ScaledObjects(ns).Get(ctx, so, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if suspend {
			result.Annotations[kedaAutoscalingPaused] = "0"
		} else {
			delete(result.Annotations, kedaAutoscalingPaused)
			delete(result.Annotations, prefix+originalScaleTargetRefName)
			if patchedName != "" {
				result.Spec.ScaleTargetRef.Name = patchedName
			}
		}

		_, err = cs.KedaV1alpha1().ScaledObjects(ns).Update(ctx, result, v1.UpdateOptions{})
		return err
	})
}
