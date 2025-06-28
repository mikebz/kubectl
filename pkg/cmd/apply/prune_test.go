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
	"bytes"
	"io"
	"net/http"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/rest/fake"
	cmdtesting "k8s.io/kubectl/pkg/cmd/testing"
	"k8s.io/kubectl/pkg/scheme"
)

const (
	testDataPath = "testdata/prune/prune-two-ns/"
	podFile11    = "prune-pod-first.yaml"
	podFile21    = "prune-pod-second-1.yaml"
	podFile22    = "prune-pod-second-2.yaml"
)

const (
	firstPodsCollection  = "/namespaces/first/pods"
	secondPodsCollection = "/namespaces/second/pods"
)

var (
	codecPrune = scheme.Codecs.LegacyCodec(scheme.Scheme.PrioritizedVersionsAllGroups()...)
)

func TestPruneTwoNamespaces(t *testing.T) {

	cmdtesting.InitTestErrorHandler(t)

	// Read the pod from the first file that will be kept
	podFirst := readUnstructuredFromFile(t, testDataPath+podFile11)

	podSecond1 := readUnstructuredFromFile(t, testDataPath+podFile21)
	podSecond2 := readUnstructuredFromFile(t, testDataPath+podFile22)

	tf := cmdtesting.NewTestFactory()
	defer tf.Cleanup()

	// these are created for simple book keeping an ease of lookup.
	// the book keeping is made in the most simple way possible to avoid creating a lot of test code.
	// the pods are specifically named so that you don't have to even keep track of a namespace.
	podMap := map[string]*unstructured.Unstructured{
		"p11": podFirst,
		"p21": podSecond1,
		"p22": podSecond2,
	}

	podExistMap := map[string]bool{
		"p11": false,
		"p21": false,
		"p22": false,
	}

	// Set up the fake REST client to handle HTTP requests
	// NOTE that a lot of the error cases are not handled here to avoid generating too much test code.
	// we would rather see a "panic" than add handlers where we don't expect
	tf.UnstructuredClient = &fake.RESTClient{
		NegotiatedSerializer: resource.UnstructuredPlusDefaultContentConfig().NegotiatedSerializer,
		Client: fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
			switch p, m := req.URL.Path, req.Method; {

			// Handle all the namespace GET's they all "exist"

			case strings.HasPrefix(p, "/api/v1/namespaces/") && m == "GET":
				namespace := strings.TrimPrefix(p, "/api/v1/namespaces/")
				// Return namespace exists
				nsResponse := `{
					"kind": "Namespace",
					"apiVersion": "v1",
					"metadata": {
						"name": "` + namespace + `"
					}
				}`
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     cmdtesting.DefaultHeader(),
					Body:       io.NopCloser(strings.NewReader(nsResponse))}, nil

			// all the LISTS
			case strings.HasPrefix(p, firstPodsCollection) && m == "GET" && strings.Contains(p, "?"):
				// Return list with p11 pod for first namespace
				pb, _ := runtime.Encode(codecPrune, podMap["p11"])
				listResponse := `{"kind":"List","apiVersion":"v1","items":[` + string(pb) + `]}`
				return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: io.NopCloser(strings.NewReader(listResponse))}, nil
			case strings.HasPrefix(p, secondPodsCollection) && m == "GET" && strings.Contains(p, "?"):
				// Handle the list response for pruning
				p1b, _ := runtime.Encode(codecPrune, podMap["p21"])
				p2b, _ := runtime.Encode(codecPrune, podMap["p22"])
				listResponse := `{"kind":"List","apiVersion":"v1","items":[` + string(p1b) + `,` + string(p2b) + `]}`
				return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: io.NopCloser(strings.NewReader(listResponse))}, nil

			// All the GETs return 404
			// the reason is that if we return the objects here kubectl decides that they already
			// exist and tries to do a patch.
			case urlInCollections(p) && (m == "GET" || m == "PATCH"):
				name := path.Base(p)

				if !podExistMap[name] {
					// If the pod does not exist, return 404
					return &http.Response{
						StatusCode: http.StatusNotFound,
						Header:     cmdtesting.DefaultHeader(),
					}, nil
				}

				pod := podMap[name]
				podBytes, _ := runtime.Encode(codecPrune, pod)
				bodyPod := io.NopCloser(bytes.NewReader(podBytes))
				return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: bodyPod}, nil

			// All the Pod POSTs
			case urlInCollections(p) && m == "POST":

				name := readPodName(t, req.Body)
				podExistMap[name] = true
				pod := podMap[name]
				setLastAppliedConfigAnnotation(podFirst)
				podBytes, _ := runtime.Encode(codecPrune, pod)
				bodyPod := io.NopCloser(bytes.NewReader(podBytes))
				return &http.Response{StatusCode: http.StatusCreated, Header: cmdtesting.DefaultHeader(), Body: bodyPod}, nil

			// All the DELETEs
			case urlInCollections(p) && m == "DELETE":
				name := path.Base(p)
				podExistMap[name] = false
				pod := podMap[name]
				podBytes, _ := runtime.Encode(codecPrune, pod)
				bodyPod := io.NopCloser(bytes.NewReader(podBytes))
				return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: bodyPod}, nil

			default:
				t.Fatalf("unexpected request: %s %s", m, p)
				return nil, nil
			}
		}),
	}
	tf.ClientConfigVal = cmdtesting.DefaultClientConfig()

	ioStreams, _, buf, errBuf := genericiooptions.NewTestIOStreams()
	cmd := NewCmdApply("kubectl", tf, ioStreams)
	cmd.Flags().Set("filename", testDataPath+podFile11)
	cmd.Flags().Set("filename", testDataPath+podFile21)
	cmd.Flags().Set("filename", testDataPath+podFile22)
	cmd.Flags().Set("output", "name")
	cmd.Run(cmd, []string{})

	if errBuf.String() != "" {
		t.Fatalf("unexpected error output: %s", errBuf.String())
	}

	// we have asked for the names to be output so we should get all of them in the output string
	outString := buf.String()
	// at this point in time none of our pods should be deleted.
	// and they all should be represented in the output
	for name, exists := range podExistMap {
		assert.True(t, exists, "pod %s should not be deleted", name)
		assert.Contains(t, outString, "pod/"+name)
	}

	// the second run should prune the p2* pods
	ioStreams, _, _, errBuf = genericiooptions.NewTestIOStreams()
	cmd = NewCmdApply("kubectl", tf, ioStreams)
	cmd.Flags().Set("filename", testDataPath+podFile11)
	cmd.Flags().Set("prune", "true")
	cmd.Flags().Set("selector", "test=managed")
	cmd.Run(cmd, []string{})
	if errBuf.String() != "" {
		t.Fatalf("unexpected error output: %s", errBuf.String())
	}

	assert.False(t, podExistMap["p21"], "pod p21 should be deleted")
	assert.False(t, podExistMap["p22"], "pod p22 should be deleted")
}

// urlInCollections checks if the given URL is part of the collections we are interested in.
func urlInCollections(url string) bool {
	return strings.HasPrefix(url, firstPodsCollection) ||
		strings.HasPrefix(url, secondPodsCollection)
}

// readUnstructuredFromFile reads a request body and returns a names
func readPodName(t *testing.T, body io.ReadCloser) string {
	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("failed to read request body: %v", err)
	}
	obj := &unstructured.Unstructured{}
	if err := runtime.DecodeInto(codecPrune, bodyBytes, obj); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return obj.GetName()
}
