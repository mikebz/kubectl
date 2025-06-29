/*
Copyright 2014 The Kubernetes Authors.

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

package apply

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest/fake"
	clientgotesting "k8s.io/client-go/testing"
	cmdtesting "k8s.io/kubectl/pkg/cmd/testing"
	"k8s.io/kubectl/pkg/scheme"
)

const (
	testDataPath = "testdata/prune/prune-two-ns/"
	podFile11    = "prune-pod-first.yaml"
	podFile21    = "prune-pod-second-1.yaml"
	podFile22    = "prune-pod-second-2.yaml"
)

func UnstructuredToNamespacePruneTest(u *unstructured.Unstructured, t *testing.T) *corev1.Namespace {
	if u == nil {
		return nil
	}
	var ns corev1.Namespace
	errConv := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &ns)
	if errConv != nil {
		t.Logf("Error converting unstructured to namespace: %v", errConv)
		return nil
	}
	return &ns
}

func TestPruneTwoNamespaces(t *testing.T) {
	cmdtesting.InitTestErrorHandler(t)

	podFirstFromFile := readUnstructuredFromFile(t, filepath.Join(testDataPath, podFile11))
	podSecond1FromFile := readUnstructuredFromFile(t, filepath.Join(testDataPath, podFile21))
	podSecond2FromFile := readUnstructuredFromFile(t, filepath.Join(testDataPath, podFile22))

	initialInTrackerPod1 := podFirstFromFile.DeepCopy()
	initialInTrackerPod1.SetUID("uid-p11")
	initialInTrackerPod2 := podSecond1FromFile.DeepCopy()
	initialInTrackerPod2.SetUID("uid-p21")
	initialInTrackerPod3 := podSecond2FromFile.DeepCopy()
	initialInTrackerPod3.SetUID("uid-p22")

	nsFirstIn := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "first", UID: "uid-ns-first"}}
	nsSecondIn := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "second", UID: "uid-ns-second"}}

	tf := cmdtesting.NewTestFactory().WithNamespace("first")
	defer tf.Cleanup()

	gvrPod := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	gvrNs := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}

	allInitialObjects := []runtime.Object{initialInTrackerPod1, initialInTrackerPod2, initialInTrackerPod3, nsFirstIn, nsSecondIn}
	tf.FakeDynamicClient = dynamicfake.NewSimpleDynamicClient(scheme.Scheme, allInitialObjects...)

	var podFirstLive, podSecond1Live, podSecond2Live *unstructured.Unstructured
	var nsFirstForHandler, nsSecondForHandler *corev1.Namespace
	// var err error // err is used by ToOptions later, avoid redeclaration if not careful

	unstrPod1, errGet1 := tf.FakeDynamicClient.Resource(gvrPod).Namespace(initialInTrackerPod1.GetNamespace()).Get(context.TODO(), initialInTrackerPod1.GetName(), metav1.GetOptions{})
	if errGet1 != nil {t.Fatalf("Failed to get pod %s from tracker: %v", initialInTrackerPod1.GetName(), errGet1)}
	podFirstLive = unstrPod1

	unstrPod2, errGet2 := tf.FakeDynamicClient.Resource(gvrPod).Namespace(initialInTrackerPod2.GetNamespace()).Get(context.TODO(), initialInTrackerPod2.GetName(), metav1.GetOptions{})
	if errGet2 != nil {t.Fatalf("Failed to get pod %s from tracker: %v", initialInTrackerPod2.GetName(), errGet2)}
	podSecond1Live = unstrPod2

	unstrPod3, errGet3 := tf.FakeDynamicClient.Resource(gvrPod).Namespace(initialInTrackerPod3.GetNamespace()).Get(context.TODO(), initialInTrackerPod3.GetName(), metav1.GetOptions{})
	if errGet3 != nil {t.Fatalf("Failed to get pod %s from tracker: %v", initialInTrackerPod3.GetName(), errGet3)}
	podSecond2Live = unstrPod3

	nsFirstLiveUntyped, errNs1 := tf.FakeDynamicClient.Resource(gvrNs).Get(context.TODO(), nsFirstIn.GetName(), metav1.GetOptions{})
	if errNs1 != nil {t.Fatalf("Failed to get ns %s from tracker: %v", nsFirstIn.GetName(), errNs1)}
	nsFirstForHandler = UnstructuredToNamespacePruneTest(nsFirstLiveUntyped, t)
	if nsFirstForHandler == nil {t.Fatalf("nsFirstForHandler is nil after conversion")}

	nsSecondLiveUntyped, errNs2 := tf.FakeDynamicClient.Resource(gvrNs).Get(context.TODO(), nsSecondIn.GetName(), metav1.GetOptions{})
	if errNs2 != nil {t.Fatalf("Failed to get ns %s from tracker: %v", nsSecondIn.GetName(), errNs2)}
	nsSecondForHandler = UnstructuredToNamespacePruneTest(nsSecondLiveUntyped, t)
	if nsSecondForHandler == nil {t.Fatalf("nsSecondForHandler is nil after conversion")}

	t.Logf("UIDs after GET from tracker: p11: %s, p21: %s, p22: %s, nsFirst: %s, nsSecond: %s",
		podFirstLive.GetUID(), podSecond1Live.GetUID(), podSecond2Live.GetUID(),
		nsFirstForHandler.GetUID(), nsSecondForHandler.GetUID())

	tf.FakeDynamicClient.PrependReactor("list", "pods", func(action clientgotesting.Action) (handled bool, ret runtime.Object, err error) {
		listAction := action.(clientgotesting.ListAction)
		t.Logf("FakeDynamicClient LIST reactor for pods: Namespace=%s, Selector=%s", listAction.GetNamespace(), listAction.GetListRestrictions().Labels.String())
		listOptions := metav1.ListOptions{
			LabelSelector: listAction.GetListRestrictions().Labels.String(),
			FieldSelector: listAction.GetListRestrictions().Fields.String(),
		}
		// Correct GVK for PodList is corev1.SchemeGroupVersion.WithKind("PodList")
		// The tracker should use this if scheme is correctly registered.
		objList, trackerErr := tf.FakeDynamicClient.Tracker().List(gvrPod, corev1.SchemeGroupVersion.WithKind("PodList"), listAction.GetNamespace(), listOptions)
		if trackerErr != nil {
			t.Logf("Error from tracker during LIST reactor: %v", trackerErr)
		} else {
			if list, ok := objList.(*unstructured.UnstructuredList); ok { // Tracker().List returns runtime.Object, cast to UnstructuredList
				t.Logf("LIST reactor: Tracker returned %d items for %s/%s selector %s", len(list.Items), listAction.GetNamespace(), "pods", listAction.GetListRestrictions().Labels.String())
			} else if list, ok := objList.(*corev1.PodList); ok { // Or it might return a typed list if scheme is good
                 t.Logf("LIST reactor: Tracker returned %d items (typed PodList) for %s/%s selector %s", len(list.Items), listAction.GetNamespace(), "pods", listAction.GetListRestrictions().Labels.String())
            }
		}
		return false, nil, nil
	})

	tf.ClientConfigVal = cmdtesting.DefaultClientConfig()

	tf.UnstructuredClient = &fake.RESTClient{
		NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
		Client: fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
			responseCodec := scheme.Codecs.LegacyCodec(corev1.SchemeGroupVersion)
			path := req.URL.Path
			t.Logf("UnstructuredClient received: Method=%s, Path=%s", req.Method, path)

			switch req.Method {
			case "GET":
				if strings.HasPrefix(path, "/namespaces/"+podFirstLive.GetNamespace()+"/pods/"+podFirstLive.GetName()) {
					t.Logf("Serving GET for podFirstLive (UID: %s)", podFirstLive.GetUID())
					return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: cmdtesting.ObjBody(responseCodec, podFirstLive)}, nil
				}
				if strings.HasPrefix(path, "/namespaces/"+podSecond1Live.GetNamespace()+"/pods/"+podSecond1Live.GetName()) {
					t.Logf("Serving GET for podSecond1Live (UID: %s)", podSecond1Live.GetUID())
					return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: cmdtesting.ObjBody(responseCodec, podSecond1Live)}, nil
				}
				if strings.HasPrefix(path, "/namespaces/"+podSecond2Live.GetNamespace()+"/pods/"+podSecond2Live.GetName()) {
					t.Logf("Serving GET for podSecond2Live (UID: %s)", podSecond2Live.GetUID())
					return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: cmdtesting.ObjBody(responseCodec, podSecond2Live)}, nil
				}
				if strings.HasPrefix(path, "/api/v1/namespaces/"+nsFirstForHandler.GetName()) && !strings.Contains(path, "/pods/") {
					t.Logf("Serving GET for nsFirstForHandler (UID: %s)", nsFirstForHandler.GetUID())
					return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: cmdtesting.ObjBody(responseCodec, nsFirstForHandler)}, nil
				}
				if strings.HasPrefix(path, "/api/v1/namespaces/"+nsSecondForHandler.GetName()) && !strings.Contains(path, "/pods/") {
					t.Logf("Serving GET for nsSecondForHandler (UID: %s)", nsSecondForHandler.GetUID())
					return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: cmdtesting.ObjBody(responseCodec, nsSecondForHandler)}, nil
				}
			case "PATCH":
				pathParts := strings.Split(strings.Trim(path, "/"), "/")
				if len(pathParts) == 4 && pathParts[0] == "namespaces" && pathParts[2] == "pods" {
					ns, name := pathParts[1], pathParts[3]
					t.Logf("PATCH Pod: ns=%s, name=%s", ns, name)
					var liveObjectToUpdate *unstructured.Unstructured

					if ns == podFirstLive.GetNamespace() && name == podFirstLive.GetName() {
						liveObjectToUpdate = podFirstLive
					} else if ns == podSecond1Live.GetNamespace() && name == podSecond1Live.GetName() {
						liveObjectToUpdate = podSecond1Live
					} else if ns == podSecond2Live.GetNamespace() && name == podSecond2Live.GetName() {
						liveObjectToUpdate = podSecond2Live
					}

					if liveObjectToUpdate != nil {
						t.Logf("Found live object to patch: %s/%s (UID before patch: %s)", ns, name, liveObjectToUpdate.GetUID())
						_, errRead := io.ReadAll(req.Body)
						if errRead != nil {
							t.Logf("Error reading patch body for %s/%s: %v", ns, name, errRead)
						}

						errAnnotation := setLastAppliedConfigAnnotation(liveObjectToUpdate)
						if errAnnotation != nil {
							t.Logf("Error setting last-applied for %s/%s: %v", ns, name, errAnnotation)
						}

						annJson, _ := json.Marshal(liveObjectToUpdate.GetAnnotations())
						t.Logf("Annotations on %s/%s after simulated patch: %s", ns, name, string(annJson))
						t.Logf("UID of %s/%s when recording update: %s", ns, name, liveObjectToUpdate.GetUID())

						updatedObjectForAction := liveObjectToUpdate.DeepCopy()
						tf.FakeDynamicClient.Fake.Invokes(clientgotesting.NewUpdateAction(gvrPod, ns, updatedObjectForAction), updatedObjectForAction)
						tf.FakeDynamicClient.Tracker().Update(gvrPod, updatedObjectForAction, ns)

						t.Logf("Patch HTTP call for %s/%s successful (simulated), action recorded, tracker updated.", ns, name)
						return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: cmdtesting.ObjBody(responseCodec, liveObjectToUpdate)}, nil
					}
					t.Logf("PATCH target object not found in live map: %s/%s", ns, name)
				}
			case "POST":
				pathParts := strings.Split(strings.Trim(path, "/"), "/")
				if len(pathParts) == 3 && pathParts[0] == "namespaces" && pathParts[2] == "pods" {
					ns := pathParts[1]
					t.Logf("POST Pod to ns=%s", ns)
					bodyBytes, errIoRead := io.ReadAll(req.Body)
					if errIoRead != nil { t.Logf("Error reading POST body: %v", errIoRead); return &http.Response{StatusCode:http.StatusInternalServerError}, nil }

					newPod := &unstructured.Unstructured{}
					errDecode := runtime.DecodeInto(scheme.Codecs.UniversalDecoder(), bodyBytes, newPod)
					if errDecode != nil { t.Logf("Error decoding POST body: %v", errDecode); return &http.Response{StatusCode:http.StatusBadRequest}, nil }

					createdObjectForAction := newPod.DeepCopy()
					errTrackerCreate := tf.FakeDynamicClient.Tracker().Create(gvrPod, createdObjectForAction, ns)
					if errTrackerCreate != nil {t.Fatalf("Tracker create failed: %v", errTrackerCreate)}
					tf.FakeDynamicClient.Fake.Invokes(clientgotesting.NewCreateAction(gvrPod, ns, createdObjectForAction), createdObjectForAction)

					if newPod.GetName() == podFirstLive.GetName() { podFirstLive = createdObjectForAction }
					if newPod.GetName() == podSecond1Live.GetName() { podSecond1Live = createdObjectForAction }
					if newPod.GetName() == podSecond2Live.GetName() { podSecond2Live = createdObjectForAction }

					t.Logf("POST for pod %s/%s successful (simulated), UID: %s", ns, newPod.GetName(), createdObjectForAction.GetUID())
					return &http.Response{StatusCode: http.StatusCreated, Header: cmdtesting.DefaultHeader(), Body: cmdtesting.ObjBody(responseCodec, createdObjectForAction)}, nil
				}
			}
			t.Logf("UnstructuredClient: Path not handled or method not supported: %s %s", req.Method, req.URL.Path)
			return &http.Response{ StatusCode: http.StatusNotFound, Header: cmdtesting.DefaultHeader(), Body:       io.NopCloser(strings.NewReader("UnstructuredClient: Path not handled: " + req.Method + " " + req.URL.Path))}, nil
		}),
	}

	// --- First Apply Run ---
	ioStreams, _, buf, errBuf := genericiooptions.NewTestIOStreams()
	firstRunApplyFlags := NewApplyFlags(ioStreams)
	firstRunCmd := &cobra.Command{}
	firstRunApplyFlags.AddFlags(firstRunCmd)

	firstRunCmd.Flags().Set("filename", filepath.Join(testDataPath, podFile11))
	firstRunCmd.Flags().Set("filename", filepath.Join(testDataPath, podFile21))
	firstRunCmd.Flags().Set("filename", filepath.Join(testDataPath, podFile22))
	firstRunCmd.Flags().Set("output", "name")

	o, err := firstRunApplyFlags.ToOptions(tf, firstRunCmd, "kubectl", []string{})
	if err != nil {
		t.Fatalf("Error creating ApplyOptions for first run: %v", err)
	}
	o.Namespace = ""
	o.EnforceNamespace = false
	o.ServerSideApply = false
	t.Logf("UIDs before first o.Run() (from live vars): p11: %s, p21: %s, p22: %s", podFirstLive.GetUID(), podSecond1Live.GetUID(), podSecond2Live.GetUID())
	if err := o.Run(); err != nil {
		if tf.FakeDynamicClient != nil {
			t.Logf("FakeDynamicClient actions at time of error (first run): %#v", tf.FakeDynamicClient.Actions())
		}
		t.Fatalf("unexpected error during first apply run: %v\nOutput: %s\nErrorOutput: %s", err, buf.String(), errBuf.String())
	}
	t.Logf("UIDs after first o.Run() (from live vars): p11: %s, p21: %s, p22: %s", podFirstLive.GetUID(), podSecond1Live.GetUID(), podSecond2Live.GetUID())
	visitedUIDsList := make([]string, 0, o.VisitedUids.Len())
	for uid := range o.VisitedUids {
		visitedUIDsList = append(visitedUIDsList, string(uid))
	}
	t.Logf("VisitedUids after first o.Run(): %v", visitedUIDsList)

	outString := buf.String()
	expectedOutputs := []string{
		"pod/" + podFirstFromFile.GetName(),
		"pod/" + podSecond1FromFile.GetName(),
		"pod/" + podSecond2FromFile.GetName(),
	}
	for _, expected := range expectedOutputs {
		assert.Contains(t, outString, expected, "Output of first apply should contain all applied pods")
	}

	firstRunActions := tf.FakeDynamicClient.Actions()
	updateCountDuringFirstRun := 0
	createCountDuringFirstRun := 0
	for _, firstRunAction := range firstRunActions {
		if firstRunAction.GetVerb() == "update" && firstRunAction.GetResource() == gvrPod {
			updateCountDuringFirstRun++
		}
		if firstRunAction.GetVerb() == "create" && firstRunAction.GetResource() == gvrPod {
			createCountDuringFirstRun++
		}
	}
	assert.Equal(t, 3, updateCountDuringFirstRun, "Expected 3 update actions for the three pods in the first apply")
	assert.Equal(t, 0, createCountDuringFirstRun, "Expected 0 create actions for the three pods in the first apply")
	tf.FakeDynamicClient.ClearActions()

	// --- Second Apply Run (with Prune) ---
	ioStreams2, _, buf2, errBuf2 := genericiooptions.NewTestIOStreams()
	secondRunApplyFlags := NewApplyFlags(ioStreams2)
	secondRunCmd := &cobra.Command{}
	secondRunApplyFlags.AddFlags(secondRunCmd)

	secondRunCmd.Flags().Set("filename", filepath.Join(testDataPath, podFile11))
	secondRunCmd.Flags().Set("prune", "true")
	secondRunCmd.Flags().Set("selector", "test=managed")

	var o2 *ApplyOptions
	o2, err = secondRunApplyFlags.ToOptions(tf, secondRunCmd, "kubectl", []string{})
	if err != nil {
		t.Fatalf("Error creating ApplyOptions for second run: %v", err)
	}
	o2.Namespace = ""
	o2.EnforceNamespace = false
	o2.ServerSideApply = false
	if errRun := o2.Run(); errRun != nil {
		if tf.FakeDynamicClient != nil {
			t.Logf("FakeDynamicClient actions at time of error (second run): %#v", tf.FakeDynamicClient.Actions())
		}
		t.Fatalf("unexpected error during second apply (prune) run: %v\nOutput: %s\nErrorOutput: %s", errRun, buf2.String(), errBuf2.String())
	}

	visitedUIDsListO2 := make([]string, 0, o2.VisitedUids.Len())
	for uid_o2 := range o2.VisitedUids { // Renamed uid to uid_o2 to avoid conflict with err variable if any
		visitedUIDsListO2 = append(visitedUIDsListO2, string(uid_o2))
	}
	t.Logf("VisitedUids after second o2.Run() (prune run): %v", visitedUIDsListO2)


	deletedPods := []string{}
	secondRunActions := tf.FakeDynamicClient.Actions()
	deleteCount := 0
	for _, action := range secondRunActions {
		if action.GetVerb() == "delete" && action.GetResource() == gvrPod {
			deleteAction := action.(clientgotesting.DeleteActionImpl)
			deletedPods = append(deletedPods, deleteAction.GetName())
			deleteCount++
		}
	}

	updateCountDuringSecondRun := 0
	for _, action := range secondRunActions {
		if action.GetVerb() == "update" && action.GetResource() == gvrPod {
			updateCountDuringSecondRun++
		}
	}
	assert.Equal(t, 1, updateCountDuringSecondRun, "Expected 1 update action for the pod in the prune run input file")

	assert.Equal(t, 2, deleteCount, "Expected 2 delete actions for the pruned pods")
	assert.Contains(t, deletedPods, podSecond1FromFile.GetName(), "podSecond1 should be pruned")
	assert.Contains(t, deletedPods, podSecond2FromFile.GetName(), "podSecond2 should be pruned")
	assert.NotContains(t, deletedPods, podFirstFromFile.GetName(), "podFirst should not be pruned")

	_, errCheck1 := tf.FakeDynamicClient.Resource(gvrPod).Namespace(podFirstLive.GetNamespace()).Get(context.TODO(), podFirstLive.GetName(), metav1.GetOptions{})
	assert.NoError(t, errCheck1, "podFirst should still exist after pruning run")

	_, errCheck2 := tf.FakeDynamicClient.Resource(gvrPod).Namespace(podSecond1Live.GetNamespace()).Get(context.TODO(), podSecond1Live.GetName(), metav1.GetOptions{})
	assert.True(t, apierrors.IsNotFound(errCheck2), "podSecond1 should be deleted after pruning run")
	_, errCheck3 := tf.FakeDynamicClient.Resource(gvrPod).Namespace(podSecond2Live.GetNamespace()).Get(context.TODO(), podSecond2Live.GetName(), metav1.GetOptions{})
	assert.True(t, apierrors.IsNotFound(errCheck3), "podSecond2 should be deleted after pruning run")
}
