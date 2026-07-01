package tests

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/rancher/steve/pkg/auth"
	"github.com/rancher/steve/pkg/server"
	"github.com/rancher/steve/pkg/sqlcache/informer/factory"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// CountsResponse mirrors the JSON shape of the /v1/counts response.
type CountsResponse struct {
	Data []struct {
		ID     string `json:"id"`
		Counts map[string]struct {
			Summary struct {
				Count int `json:"count"`
			} `json:"summary"`
		} `json:"counts"`
	} `json:"data"`
}

// bananaGVR is the GVR for the fruits.cattle.io Banana CRD that is installed
// during SetupSuite from testdata/crds/bananas.fruits.cattle.io.yaml.
var bananaGVR = schema.GroupVersionResource{
	Group:    "fruits.cattle.io",
	Version:  "v1",
	Resource: "bananas",
}

// bananaSchemaID is the schema ID steve assigns: group.kind (singular, lowercase).
// This is different from the plural resource name used in URL paths.
const bananaSchemaID = "fruits.cattle.io.banana"

// getCountForSchema fetches /v1/counts and returns the object count for the
// given schemaID.  Returns -1 if the schema is not present in the response.
func getCountForSchema(baseURL, schemaID string) (int, error) {
	resp, err := http.Get(baseURL + "/v1/counts")
	if err != nil {
		return -1, err
	}
	defer resp.Body.Close()

	var cr CountsResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		return -1, err
	}
	for _, item := range cr.Data {
		if item.ID != "count" {
			continue
		}
		for id, ic := range item.Counts {
			if id == schemaID {
				return ic.Summary.Count, nil
			}
		}
		return -1, nil // schema not present
	}
	return -1, nil
}

// TestCountsWatchableResource verifies that /v1/counts accurately reflects the
// number of CRD objects that exist, and that counts update when objects are
// created and deleted.
//
// This exercises the ClusterCache-backed path (resources that support the watch
// verb).  The store-fallback path for non-watchable resources (e.g. aggregated
// API servers that omit the watch verb) is covered by unit tests in
// pkg/resources/counts/counts_test.go.
func (i *IntegrationSuite) TestCountsWatchableResource() {
	ctx := i.T().Context()

	steveHandler, err := server.New(ctx, i.restCfg, &server.Options{
		SQLCache: true,
		SQLCacheFactoryOptions: factory.CacheFactoryOptions{
			GCInterval:  15 * time.Minute,
			GCKeepCount: 1000,
		},
		AuthMiddleware: auth.ToMiddleware(auth.AuthenticatorFunc(auth.AlwaysAdmin)),
	})
	i.Require().NoError(err)

	httpServer := httptest.NewServer(steveHandler)
	defer httpServer.Close()

	baseURL := httpServer.URL

	// Wait until steve has discovered and registered the Banana schema.
	i.waitForSchema(baseURL, bananaGVR)

	defer i.maybeStopAndDebug(baseURL)

	// --- banana schema must appear in /v1/counts ---
	var initialCount int
	i.Require().EventuallyWithT(func(c *assert.CollectT) {
		count, err := getCountForSchema(baseURL, bananaSchemaID)
		if !assert.NoError(c, err) {
			return
		}
		assert.GreaterOrEqual(c, count, 0, "banana schema should appear in /v1/counts")
		initialCount = count
	}, 15*time.Second, 200*time.Millisecond)

	// --- create two Banana objects ---
	banana1 := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "fruits.cattle.io/v1",
			"kind":       "Banana",
			"metadata":   map[string]any{"name": "counts-test-banana-1"},
			"color":      "yellow",
		},
	}
	banana2 := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "fruits.cattle.io/v1",
			"kind":       "Banana",
			"metadata":   map[string]any{"name": "counts-test-banana-2"},
			"color":      "green",
		},
	}

	_, err = i.client.Resource(bananaGVR).Apply(ctx, "counts-test-banana-1", banana1, metav1.ApplyOptions{FieldManager: "integration-tests"})
	i.Require().NoError(err)
	defer i.client.Resource(bananaGVR).Delete(ctx, "counts-test-banana-1", metav1.DeleteOptions{})

	_, err = i.client.Resource(bananaGVR).Apply(ctx, "counts-test-banana-2", banana2, metav1.ApplyOptions{FieldManager: "integration-tests"})
	i.Require().NoError(err)
	defer i.client.Resource(bananaGVR).Delete(ctx, "counts-test-banana-2", metav1.DeleteOptions{})

	// --- /v1/counts must reflect the two new objects ---
	expectedAfterCreate := initialCount + 2
	i.Require().EventuallyWithT(func(c *assert.CollectT) {
		count, err := getCountForSchema(baseURL, bananaSchemaID)
		assert.NoError(c, err)
		assert.Equal(c, expectedAfterCreate, count, "count should include newly created bananas")
	}, 15*time.Second, 200*time.Millisecond)

	// --- delete one Banana and verify count drops ---
	err = i.client.Resource(bananaGVR).Delete(ctx, "counts-test-banana-1", metav1.DeleteOptions{})
	i.Require().NoError(err)

	expectedAfterDelete := expectedAfterCreate - 1
	i.Require().EventuallyWithT(func(c *assert.CollectT) {
		count, err := getCountForSchema(baseURL, bananaSchemaID)
		assert.NoError(c, err)
		assert.Equal(c, expectedAfterDelete, count, "count should drop after deleting a banana")
	}, 15*time.Second, 200*time.Millisecond)
}
