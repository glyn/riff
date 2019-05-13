package commands_test

import (
	"github.com/projectriff/riff/pkg/riff/commands"
	"github.com/projectriff/riff/pkg/testing"
	buildv1alpha1 "github.com/projectriff/system/pkg/apis/build/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestApplicationDeleteOptions(t *testing.T) {
	table := testing.OptionsTable{
		{
			Name: "invalid delete, missing",
			Options: &commands.ApplicationDeleteOptions{
				DeleteOptions: testing.InvalidDeleteOptionsMissing,
			},
			ExpectFieldError: testing.InvalidDeleteOptionsFieldMissingError,
		},
		{
			Name: "invalid delete, excessive",
			Options: &commands.ApplicationDeleteOptions{
				DeleteOptions: testing.InvalidDeleteOptionsExcess,
			},
			ExpectFieldError: testing.InvalidDeleteOptionsFieldExtraError,
		},
		{
			Name: "valid delete",
			Options: &commands.ApplicationDeleteOptions{
				DeleteOptions: testing.ValidDeleteOptions,
			},
			ShouldValidate: true,
		},
	}

	table.Run(t)
}

func TestApplicationDeleteCommand(t *testing.T) {
	t.Parallel()

	functionName := "test-function"
	functionAltName := "test-alt-function"
	defaultNamespace := "default"

	table := testing.CommandTable{
		{
			Name: "delete all functions",
			Args: []string{"--all"},
			GivenObjects: []runtime.Object{
				&buildv1alpha1.Function{
					ObjectMeta: metav1.ObjectMeta{
						Name:      functionName,
						Namespace: defaultNamespace,
					},
				},
			},
			ExpectDeleteCollections: []testing.DeleteCollectionRef{{
				Group:     "build.projectriff.io",
				Resource:  "functions",
				Namespace: defaultNamespace,
			}},
		},
		{
			Name: "delete function",
			Args: []string{functionName},
			GivenObjects: []runtime.Object{
				&buildv1alpha1.Function{
					ObjectMeta: metav1.ObjectMeta{
						Name:      functionName,
						Namespace: defaultNamespace,
					},
				},
			},
			ExpectDeletes: []testing.DeleteRef{{
				Group:     "build.projectriff.io",
				Resource:  "functions",
				Namespace: defaultNamespace,
				Name:      functionName,
			}},
		},
		{
			Name: "delete functions",
			Args: []string{functionName, functionAltName},
			GivenObjects: []runtime.Object{
				&buildv1alpha1.Function{
					ObjectMeta: metav1.ObjectMeta{
						Name:      functionName,
						Namespace: defaultNamespace,
					},
				},
				&buildv1alpha1.Function{
					ObjectMeta: metav1.ObjectMeta{
						Name:      functionAltName,
						Namespace: defaultNamespace,
					},
				},
			},
			ExpectDeletes: []testing.DeleteRef{{
				Group:     "build.projectriff.io",
				Resource:  "functions",
				Namespace: defaultNamespace,
				Name:      functionName,
			}, {
				Group:     "build.projectriff.io",
				Resource:  "functions",
				Namespace: defaultNamespace,
				Name:      functionAltName,
			}},
		},
		{
			Name: "function does not exist",
			Args: []string{functionName},
			ExpectDeletes: []testing.DeleteRef{{
				Group:     "build.projectriff.io",
				Resource:  "functions",
				Namespace: defaultNamespace,
				Name:      functionName,
			}},
			ShouldError: true,
		},
		{
			Name: "delete error",
			Args: []string{functionName},
			GivenObjects: []runtime.Object{
				&buildv1alpha1.Function{
					ObjectMeta: metav1.ObjectMeta{
						Name:      functionName,
						Namespace: defaultNamespace,
					},
				},
			},
			WithReactors: []testing.ReactionFunc{
				testing.InduceFailure("delete", "functions"),
			},
			ExpectDeletes: []testing.DeleteRef{{
				Group:     "build.projectriff.io",
				Resource:  "functions",
				Namespace: defaultNamespace,
				Name:      functionName,
			}},
			ShouldError: true,
		},
	}

	table.Run(t, commands.NewApplicationDeleteCommand)
}

