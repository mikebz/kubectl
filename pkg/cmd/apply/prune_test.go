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

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/rest/fake"
	cmdtesting "k8s.io/kubectl/pkg/cmd/testing"
	"k8s.io/kubectl/pkg/scheme"
)

const (
	testDataPath = "testdata/prune/prune-two-ns/"
	podFirstNs   = "prune-pod-first.yaml"
	podSecondNs1 = "prune-pod-second-1.yaml"
	podSecondNs2 = "prune-pod-second-2.yaml"
	nameFirst    = "default-http-1"
	nameSecond   = "default-http-1"
)

var (
	codecPrune = scheme.Codecs.LegacyCodec(scheme.Scheme.PrioritizedVersionsAllGroups()...)
)

func TestPruneTwoNamespaces(t *testing.T) {
	cmdtesting.InitTestErrorHandler(t)

	// Read the pod from the first file that will be kept
	podFirst := readUnstructuredFromFile(t, testDataPath+podFirstNs)
	pathFirstPod := "/namespaces/first/pods/" + nameFirst
	pathFirstPodsCollection := "/namespaces/first/pods"

	tf := cmdtesting.NewTestFactory()
	defer tf.Cleanup()

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
			case p == pathFirstPod && m == "GET":
				// Return 404 to simulate the pod doesn't exist yet
				return &http.Response{StatusCode: http.StatusNotFound, Header: cmdtesting.DefaultHeader(), Body: io.NopCloser(strings.NewReader(""))}, nil
			case p == pathFirstPodsCollection && m == "POST":
				// Create the pod in first namespace
				podBytes, _ := runtime.Encode(codecPrune, podFirst)
				bodyPod := io.NopCloser(bytes.NewReader(podBytes))
				return &http.Response{StatusCode: http.StatusCreated, Header: cmdtesting.DefaultHeader(), Body: bodyPod}, nil
			case strings.HasPrefix(p, "/namespaces/first/pods") && m == "GET" && strings.Contains(p, "?"):
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
	cmd.Flags().Set("filename", testDataPath+podFirstNs)
	cmd.Flags().Set("output", "name")
	cmd.Run(cmd, []string{})

	// Check that the pod was applied successfully
	expectedOutput := "pod/" + nameFirst + "\n"
	if buf.String() != expectedOutput {
		t.Fatalf("unexpected output: %s\nexpected: %s", buf.String(), expectedOutput)
	}
	if errBuf.String() != "" {
		t.Fatalf("unexpected error output: %s", errBuf.String())
	}
}
