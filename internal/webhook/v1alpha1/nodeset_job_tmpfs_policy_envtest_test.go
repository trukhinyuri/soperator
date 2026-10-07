//go:build envtest

package v1alpha1

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	slurmv1alpha1 "nebius.ai/slurm-operator/api/v1alpha1"
)

const jobTmpfsPolicyPath = "../../../helm/soperator/templates/nodeset-job-tmpfs-admission-policy.yaml"

func TestNodeSetJobTmpfsAdmissionPolicy(t *testing.T) {
	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := testEnv.Start()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, testEnv.Stop()) })

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, slurmv1alpha1.AddToScheme(scheme))
	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)

	ctx := context.Background()
	for _, object := range readPolicyObjects(t) {
		require.NoError(t, k8sClient.Create(ctx, object))
	}
	require.NoError(t, k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "no-cluster"}}))
	require.NoError(t, k8sClient.Create(ctx, newPolicyTestSlurmCluster("enabled", nil)))
	disabled := false
	require.NoError(t, k8sClient.Create(ctx, newPolicyTestSlurmCluster("disabled", &disabled)))

	// Policies and their params propagate to admission asynchronously. A CEL compile error would
	// reject every request under failurePolicy: Fail, so the allowed cases below also cover it.
	require.Eventually(t, func() bool {
		err := k8sClient.Create(ctx, newPolicyTestNodeSet("default", "enabled", "/tmp"), client.DryRunAll)
		return apierrors.IsInvalid(err)
	}, time.Minute, 200*time.Millisecond, "policy never started rejecting shadowed jail submounts")

	for _, tc := range []struct {
		name       string
		namespace  string
		cluster    string
		mountPaths []string
		rejected   string
	}{
		{name: "tmp", namespace: "default", cluster: "enabled", mountPaths: []string{"/tmp"}, rejected: "/tmp"},
		{name: "below shm", namespace: "default", cluster: "enabled", mountPaths: []string{"/dev//shm/nccl"}, rejected: "/dev//shm/nccl"},
		{name: "below memory", namespace: "default", cluster: "enabled", mountPaths: []string{"/mnt/local", "/mnt/memory/data/"}, rejected: "/mnt/memory/data/"},
		{name: "unrelated paths", namespace: "default", cluster: "enabled", mountPaths: []string{"/mnt/local", "/tmpdata", "/mnt", "/dev"}},
		{name: "job tmpfs disabled", namespace: "default", cluster: "disabled", mountPaths: []string{"/tmp"}},
		{name: "other cluster name", namespace: "default", cluster: "missing", mountPaths: []string{"/tmp"}},
		{name: "no cluster in namespace", namespace: "no-cluster", cluster: "enabled", mountPaths: []string{"/tmp"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := k8sClient.Create(ctx, newPolicyTestNodeSet(tc.namespace, tc.cluster, tc.mountPaths...), client.DryRunAll)
			if tc.rejected == "" {
				require.NoError(t, err)
				return
			}
			require.True(t, apierrors.IsInvalid(err), "expected rejection, got %v", err)
			assert.Contains(t, err.Error(), "jailSubMounts "+tc.rejected+" are hidden from jobs")
			assert.Contains(t, err.Error(), "SlurmCluster enabled")
		})
	}

	t.Run("update", func(t *testing.T) {
		nodeSet := newPolicyTestNodeSet("default", "enabled", "/mnt/local")
		require.NoError(t, k8sClient.Create(ctx, nodeSet))
		nodeSet.Spec.Slurmd.Volumes.JailSubMounts[0].MountPath = "/tmp/scratch"
		err := k8sClient.Update(ctx, nodeSet)
		require.True(t, apierrors.IsInvalid(err), "expected rejection, got %v", err)
	})
}

func readPolicyObjects(t *testing.T) []*unstructured.Unstructured {
	t.Helper()
	content, err := os.ReadFile(jobTmpfsPolicyPath)
	require.NoError(t, err)
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(content), 4096)
	var objects []*unstructured.Unstructured
	for {
		object := &unstructured.Unstructured{}
		if err := decoder.Decode(&object.Object); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
		}
		if len(object.Object) > 0 {
			objects = append(objects, object)
		}
	}
	require.Len(t, objects, 2)
	return objects
}

func newPolicyTestSlurmCluster(name string, jobTmpfsEnabled *bool) *unstructured.Unstructured {
	spec := map[string]any{
		"k8sNodeFilters": []any{map[string]any{"name": "nodes"}},
		"populateJail":   map[string]any{"k8sNodeFilterName": "nodes", "image": "populate-jail"},
		"secrets":        map[string]any{},
		"slurmNodes": map[string]any{
			"accounting": map[string]any{"k8sNodeFilterName": "nodes"},
			"controller": map[string]any{
				"k8sNodeFilterName": "nodes",
				"munge":             map[string]any{"image": "munge"},
				"slurmctld":         map[string]any{"image": "slurmctld"},
				"volumes":           map[string]any{"jail": map[string]any{}, "spool": map[string]any{}},
			},
			"exporter": map[string]any{"k8sNodeFilterName": "nodes"},
			"login": map[string]any{
				"k8sNodeFilterName": "nodes",
				"munge":             map[string]any{"image": "munge"},
				"sshRootPublicKeys": []any{},
				"sshd":              map[string]any{"image": "sshd"},
				"sshdServiceType":   "ClusterIP",
				"volumes":           map[string]any{"jail": map[string]any{}},
			},
			"rest": map[string]any{"k8sNodeFilterName": "nodes"},
		},
	}
	if jobTmpfsEnabled != nil {
		spec["jobTmpfs"] = map[string]any{"enabled": *jobTmpfsEnabled}
	}
	cluster := &unstructured.Unstructured{Object: map[string]any{"spec": spec}}
	cluster.SetAPIVersion("slurm.nebius.ai/v1")
	cluster.SetKind("SlurmCluster")
	cluster.SetNamespace("default")
	cluster.SetName(name)
	return cluster
}

func newPolicyTestNodeSet(namespace, clusterName string, mountPaths ...string) *slurmv1alpha1.NodeSet {
	var mounts []slurmv1alpha1.NodeVolumeMount
	for _, mountPath := range mountPaths {
		mounts = append(mounts, slurmv1alpha1.NodeVolumeMount{Name: "mount", MountPath: mountPath})
	}
	return &slurmv1alpha1.NodeSet{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "workers-", Namespace: namespace},
		Spec: slurmv1alpha1.NodeSetSpec{
			ClusterName:    clusterName,
			UpdateStrategy: "rollingUpdate",
			Munge: slurmv1alpha1.ContainerMungeSpec{
				Image:     slurmv1alpha1.Image{Repository: "munge"},
				Resources: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
			},
			Slurmd: slurmv1alpha1.ContainerSlurmdSpec{
				Image:     slurmv1alpha1.Image{Repository: "slurmd"},
				Resources: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
				Volumes:   slurmv1alpha1.WorkerVolumesSpec{JailSubMounts: mounts},
			},
		},
	}
}
