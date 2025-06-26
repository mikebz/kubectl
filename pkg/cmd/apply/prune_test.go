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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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

var (
	codecPrune = scheme.Codecs.LegacyCodec(scheme.Scheme.PrioritizedVersionsAllGroups()...)
)

func TestPruneTwoNamespaces(t *testing.T) {

	cmdtesting.InitTestErrorHandler(t)

	// Read the pod from the first file that will be kept
	podFirst := readUnstructuredFromFile(t, testDataPath+podFile11)
	urlFirstPod := "/namespaces/first/pods/p11"
	firstPodsCollection := "/namespaces/first/pods"
	podSecond1 := readUnstructuredFromFile(t, testDataPath+podFile21)
	podSecond2 := readUnstructuredFromFile(t, testDataPath+podFile22)
	urlSecondPod1 := "/namespaces/second/pods/p21"
	urlSecondPod2 := "/namespaces/second/pods/p22"
	secondPodsCollection := "/namespaces/second/pods"

	tf := cmdtesting.NewTestFactory()
	defer tf.Cleanup()

	firstPodDeleted := false
	secondPod1Deleted := false
	secondPod2Deleted := false

	// Set up the fake REST client to handle HTTP requests
	tf.UnstructuredClient = &fake.RESTClient{
		NegotiatedSerializer: resource.UnstructuredPlusDefaultContentConfig().NegotiatedSerializer,
		Client: fake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
			switch p, m := req.URL.Path, req.Method; {
			case p == "/api/v1/namespaces/first" && m == "GET":
				// Return namespace exists
				nsResponse := `{
					"kind": "Namespace",
					"apiVersion": "v1",
					"metadata": {
						"name": "first"
					}
				}`
				return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: io.NopCloser(strings.NewReader(nsResponse))}, nil
			case p == "/api/v1/namespaces/second" && m == "GET":
				// Return namespace exists
				nsResponse := `{
					"kind": "Namespace",
					"apiVersion": "v1",
					"metadata": {
						"name": "second"
					}
				}`
				return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: io.NopCloser(strings.NewReader(nsResponse))}, nil

			// All the GETs
			case p == urlFirstPod && (m == "GET" || m == "PATCH"):
				podBytes, _ := runtime.Encode(codecPrune, podFirst)
				bodyPod := io.NopCloser(bytes.NewReader(podBytes))
				return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: bodyPod}, nil
			case p == urlSecondPod1 && (m == "GET" || m == "PATCH"):
				podBytes, _ := runtime.Encode(codecPrune, podSecond1)
				bodyPod := io.NopCloser(bytes.NewReader(podBytes))
				return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: bodyPod}, nil
			case p == urlSecondPod2 && (m == "GET" || m == "PATCH"):
				podBytes, _ := runtime.Encode(codecPrune, podSecond2)
				bodyPod := io.NopCloser(bytes.NewReader(podBytes))
				return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: bodyPod}, nil

			// All the POSTs
			case p == firstPodsCollection && m == "POST":
				// Create the pod in first namespace
				podBytes, _ := runtime.Encode(codecPrune, podFirst)
				bodyPod := io.NopCloser(bytes.NewReader(podBytes))
				return &http.Response{StatusCode: http.StatusCreated, Header: cmdtesting.DefaultHeader(), Body: bodyPod}, nil
			case p == secondPodsCollection && m == "POST":
				// Create the pod in second namespace
				podBytes, _ := runtime.Encode(codecPrune, podSecond1)
				bodyPod := io.NopCloser(bytes.NewReader(podBytes))
				return &http.Response{StatusCode: http.StatusCreated, Header: cmdtesting.DefaultHeader(), Body: bodyPod}, nil

			// All the DELETE
			case p == urlFirstPod && m == "DELETE":
				// Delete the first pod in second namespace
				firstPodDeleted = true
				podBytes, _ := runtime.Encode(codecPrune, podFirst)
				bodyPod := io.NopCloser(bytes.NewReader(podBytes))
				return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: bodyPod}, nil
			case p == urlSecondPod1 && m == "DELETE":
				// Delete the first pod in second namespace
				secondPod1Deleted = true
				podBytes, _ := runtime.Encode(codecPrune, podSecond1)
				bodyPod := io.NopCloser(bytes.NewReader(podBytes))
				return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: bodyPod}, nil
			case p == urlSecondPod2 && m == "DELETE":
				// Delete the second pod in second namespace
				secondPod2Deleted = true
				podBytes, _ := runtime.Encode(codecPrune, podSecond2)
				bodyPod := io.NopCloser(bytes.NewReader(podBytes))
				return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: bodyPod}, nil

			// all the LISTS
			case strings.HasPrefix(p, firstPodsCollection) && m == "GET" && strings.Contains(p, "?"):
				// Handle list requests for pruning - return empty lists
				return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: io.NopCloser(strings.NewReader(`{"kind":"List","apiVersion":"v1","items":[]}`))}, nil
			case strings.HasPrefix(p, secondPodsCollection) && m == "GET" && strings.Contains(p, "?"):
				// Handle list requests for pruning - return empty lists
				return &http.Response{StatusCode: http.StatusOK, Header: cmdtesting.DefaultHeader(), Body: io.NopCloser(strings.NewReader(`{"kind":"List","apiVersion":"v1","items":[]}`))}, nil

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

	// we have asked for the names to be output so we should get all of them in the output string
	outString := buf.String()
	assert.Contains(t, outString, "pod/p11")
	assert.Contains(t, outString, "pod/p21")
	assert.Contains(t, outString, "pod/p22")
	if errBuf.String() != "" {
		t.Fatalf("unexpected error output: %s", errBuf.String())
	}

	// at this point in time none of our pods should be deleted.
	assert.False(t, firstPodDeleted, "first pod should not be deleted")
	assert.False(t, secondPod1Deleted, "second pod 1 should not be deleted")
	assert.False(t, secondPod2Deleted, "second pod 2 should not be deleted")
}
