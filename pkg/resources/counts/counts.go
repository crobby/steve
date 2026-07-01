package counts

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/rancher/apiserver/pkg/store/empty"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/attributes"
	"github.com/rancher/steve/pkg/clustercache"
	"github.com/rancher/wrangler/v3/pkg/schemas"
	"github.com/rancher/wrangler/v3/pkg/summary"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	schema2 "k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	ignore = map[string]bool{
		"count":   true,
		"schema":  true,
		"apiRoot": true,
	}
)

// nonWatchablePollInterval is how often the Watch stream re-lists resources
// that do not support the watch verb (e.g. aggregated-API action resources).
const nonWatchablePollInterval = 30 * time.Second

// Register registers a new count schema. This schema isn't a true resource but instead returns counts for other resources
func Register(schemas *types.APISchemas, ccache clustercache.ClusterCache) {
	schemas.MustImportAndCustomize(Count{}, func(schema *types.APISchema) {
		schema.CollectionMethods = []string{http.MethodGet}
		schema.ResourceMethods = []string{http.MethodGet}
		schema.Attributes["access"] = accesscontrol.AccessListByVerb{
			"watch": accesscontrol.AccessList{
				accesscontrol.Access{
					Namespace:    "*",
					ResourceName: "*",
				},
			},
		}
		schema.Store = &Store{
			ccache: ccache,
		}
	})
}

type Count struct {
	ID     string               `json:"id,omitempty"`
	Counts map[string]ItemCount `json:"counts"`
}

type Summary struct {
	Count         int            `json:"count,omitempty"`
	States        map[string]int `json:"states,omitempty"`
	Error         int            `json:"errors,omitempty"`
	Transitioning int            `json:"transitioning,omitempty"`
}

func (s *Summary) DeepCopy() *Summary {
	r := *s
	if r.States != nil {
		r.States = map[string]int{}
		for k := range s.States {
			r.States[k] = s.States[k]
		}
	}
	return &r
}

type ItemCount struct {
	Summary    Summary            `json:"summary,omitempty"`
	Namespaces map[string]Summary `json:"namespaces,omitempty"`
	Revision   int                `json:"-"`
}

func (i *ItemCount) DeepCopy() *ItemCount {
	r := *i
	r.Summary = *r.Summary.DeepCopy()
	if r.Namespaces != nil {
		r.Namespaces = map[string]Summary{}
		for k, v := range i.Namespaces {
			r.Namespaces[k] = *v.DeepCopy()
		}
	}
	return &r
}

type Store struct {
	empty.Store
	ccache clustercache.ClusterCache
}

func toAPIObject(c Count) types.APIObject {
	return types.APIObject{
		Type:   "count",
		ID:     c.ID,
		Object: c,
	}
}

func (s *Store) ByID(apiOp *types.APIRequest, schema *types.APISchema, id string) (types.APIObject, error) {
	c := s.getCount(apiOp)
	return toAPIObject(c), nil
}

func (s *Store) List(apiOp *types.APIRequest, schema *types.APISchema) (types.APIObjectList, error) {
	c := s.getCount(apiOp)
	return types.APIObjectList{
		Objects: []types.APIObject{
			toAPIObject(c),
		},
	}, nil
}

// Watch creates a watch for the Counts schema. This returns only the counts which have changed since the watch was established
func (s *Store) Watch(apiOp *types.APIRequest, schema *types.APISchema, w types.WatchRequest) (chan types.APIEvent, error) {
	var (
		result      = make(chan Count, 100)
		counts      map[string]ItemCount
		gvkToSchema = map[schema2.GroupVersionKind]*types.APISchema{}
		countLock   sync.Mutex
	)

	go func() {
		<-apiOp.Context().Done()
		countLock.Lock()
		close(result)
		result = nil
		countLock.Unlock()
	}()

	counts = s.getCount(apiOp).Counts
	for id := range counts {
		schema := apiOp.Schemas.LookupSchema(id)
		if schema == nil {
			continue
		}

		gvkToSchema[attributes.GVK(schema)] = schema
	}

	onChange := func(add bool, gvk schema2.GroupVersionKind, _ string, obj, oldObj runtime.Object) error {
		countLock.Lock()
		defer countLock.Unlock()

		if result == nil {
			return nil
		}

		schema := gvkToSchema[gvk]
		if schema == nil {
			return nil
		}

		_, namespace, revision, summary, ok := getInfo(obj, schema)
		if !ok {
			return nil
		}

		itemCount := counts[schema.ID]
		if revision <= itemCount.Revision {
			return nil
		}

		if oldObj != nil {
			if _, _, _, oldSummary, ok := getInfo(oldObj, schema); ok {
				if oldSummary.Transitioning == summary.Transitioning &&
					oldSummary.Error == summary.Error &&
					simpleState(oldSummary) == simpleState(summary) {
					return nil
				}
				itemCount = removeCounts(itemCount, namespace, oldSummary)
				itemCount = addCounts(itemCount, namespace, summary)
			} else {
				return nil
			}
		} else if add {
			itemCount = addCounts(itemCount, namespace, summary)
		} else {
			itemCount = removeCounts(itemCount, namespace, summary)
		}

		counts[schema.ID] = itemCount
		changedCount := map[string]ItemCount{
			schema.ID: *itemCount.DeepCopy(),
		}

		result <- Count{
			ID:     "count",
			Counts: changedCount,
		}

		return nil
	}

	s.ccache.OnAdd(apiOp.Context(), func(gvk schema2.GroupVersionKind, key string, obj runtime.Object) error {
		return onChange(true, gvk, key, obj, nil)
	})
	s.ccache.OnChange(apiOp.Context(), func(gvk schema2.GroupVersionKind, key string, obj, oldObj runtime.Object) error {
		return onChange(true, gvk, key, obj, oldObj)
	})
	s.ccache.OnRemove(apiOp.Context(), func(gvk schema2.GroupVersionKind, key string, obj runtime.Object) error {
		return onChange(false, gvk, key, obj, nil)
	})

	// For resources that don't support the watch verb the ClusterCache has no
	// informer, so onChange will never fire for them.  Poll their store
	// directly so the Watch stream stays live for non-watchable schemas too.
	for id := range counts {
		pollSchema := apiOp.Schemas.LookupSchema(id)
		if pollSchema == nil || hasListAndWatch(pollSchema) {
			continue // ClusterCache handles these via onChange above
		}
		pollID := id
		go func() {
			ticker := time.NewTicker(nonWatchablePollInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					newItemCount, err := s.listFromStore(apiOp, pollSchema)
					if err != nil {
						logrus.Debugf("counts: poll list failed for %s: %v", pollID, err)
						continue
					}
					countLock.Lock()
					if result == nil {
						countLock.Unlock()
						return
					}
					prev := counts[pollID]
					if newItemCount.Summary.Count != prev.Summary.Count {
						counts[pollID] = newItemCount
						result <- Count{
							ID:     "count",
							Counts: map[string]ItemCount{pollID: *newItemCount.DeepCopy()},
						}
					}
					countLock.Unlock()
				case <-apiOp.Context().Done():
					return
				}
			}
		}()
	}

	// buffer the counts so that we don't spam the consumer with constant updates
	return countsBuffer(result), nil
}

func (s *Store) schemasToWatch(apiOp *types.APIRequest) (result []*types.APISchema) {
	for _, schema := range apiOp.Schemas.Schemas {
		if ignore[schema.ID] {
			continue
		}

		if schema.Store == nil {
			continue
		}

		if apiOp.AccessControl.CanList(apiOp, schema) != nil {
			continue
		}

		if apiOp.AccessControl.CanWatch(apiOp, schema) != nil {
			continue
		}

		result = append(result, schema)
	}

	return
}

// hasListAndWatch reports whether the schema's resource advertises both the
// "list" and "watch" verbs, which are required for cluster-cache informers.
// Resources that lack watch (e.g. some aggregated-API action resources) have
// no informer; their counts are obtained via listFromStore fallback instead.
func hasListAndWatch(schema *types.APISchema) bool {
	canList, canWatch := false, false
	for _, verb := range attributes.Verbs(schema) {
		switch verb {
		case "list":
			canList = true
		case "watch":
			canWatch = true
		}
	}
	return canList && canWatch
}

// listFromStore lists all objects for the schema via its Store and returns
// an ItemCount with per-namespace totals.  It is used as a fallback for
// resources whose API does not support the watch verb (and therefore have no
// ClusterCache informer backing them).  Summary state (error/transitioning)
// is not available through this path and is left as zero.
func (s *Store) listFromStore(apiOp *types.APIRequest, schema *types.APISchema) (ItemCount, error) {
	itemCount := ItemCount{Namespaces: map[string]Summary{}}
	if schema.Store == nil {
		return itemCount, nil
	}

	// Shallow-copy the request, overriding Namespace so we list across all
	// namespaces regardless of whatever namespace the caller targeted.
	listOp := *apiOp
	listOp.Namespace = ""

	result, err := schema.Store.List(&listOp, schema)
	if err != nil {
		return itemCount, fmt.Errorf("listing %s for counts: %w", schema.ID, err)
	}

	for _, obj := range result.Objects {
		ns := namespaceFromObject(obj)
		itemCount = addCounts(itemCount, ns, summary.Summary{})
	}
	return itemCount, nil
}

// namespaceFromObject extracts the namespace from a types.APIObject by
// inspecting its underlying runtime.Object via the meta accessor.
func namespaceFromObject(obj types.APIObject) string {
	if obj.Object == nil {
		return ""
	}
	r, ok := obj.Object.(runtime.Object)
	if !ok {
		return ""
	}
	m, err := meta.Accessor(r)
	if err != nil {
		return ""
	}
	return m.GetNamespace()
}

func getInfo(obj interface{}, schema *types.APISchema) (name string, namespace string, revision int, summaryResult summary.Summary, ok bool) {
	r, ok := obj.(runtime.Object)
	if !ok {
		return "", "", 0, summaryResult, false
	}

	meta, err := meta.Accessor(r)
	if err != nil {
		return "", "", 0, summaryResult, false
	}

	revision, err = strconv.Atoi(meta.GetResourceVersion())
	if err != nil {
		return "", "", 0, summaryResult, false
	}

	opts := &summary.SummarizeOptions{HasObservedGeneration: false}
	if schema != nil && schema.Attributes != nil {
		opts.HasObservedGeneration = schemas.HasObservedGeneration(schema.Schema)
	}

	summaryResult = summary.SummarizeWithOptions(r, opts)
	return meta.GetName(), meta.GetNamespace(), revision, summaryResult, true
}

func removeCounts(itemCount ItemCount, ns string, summary summary.Summary) ItemCount {
	itemCount.Summary = removeSummary(itemCount.Summary, summary)
	if ns != "" {
		itemCount.Namespaces[ns] = removeSummary(itemCount.Namespaces[ns], summary)
	}
	return itemCount
}

func addCounts(itemCount ItemCount, ns string, summary summary.Summary) ItemCount {
	itemCount.Summary = addSummary(itemCount.Summary, summary)
	if ns != "" {
		itemCount.Namespaces[ns] = addSummary(itemCount.Namespaces[ns], summary)
	}
	return itemCount
}

func removeSummary(counts Summary, summary summary.Summary) Summary {
	counts.Count--
	if summary.Transitioning {
		counts.Transitioning--
	}
	if summary.Error {
		counts.Error--
	}
	if simpleState(summary) != "" {
		if counts.States == nil {
			counts.States = map[string]int{}
		}
		counts.States[simpleState(summary)]--
	}
	return counts
}

func addSummary(counts Summary, summary summary.Summary) Summary {
	counts.Count++
	if summary.Transitioning {
		counts.Transitioning++
	}
	if summary.Error {
		counts.Error++
	}
	if simpleState(summary) != "" {
		if counts.States == nil {
			counts.States = map[string]int{}
		}
		counts.States[simpleState(summary)]++
	}
	return counts
}

func simpleState(summary summary.Summary) string {
	if summary.Error {
		return "error"
	} else if summary.Transitioning {
		return "in-progress"
	}
	return ""
}

func (s *Store) getCount(apiOp *types.APIRequest) Count {
	counts := map[string]ItemCount{}

	for _, schema := range s.schemasToWatch(apiOp) {
		gvk := attributes.GVK(schema)
		access, _ := attributes.Access(schema).(accesscontrol.AccessListByVerb)

		rev := 0
		itemCount := ItemCount{
			Namespaces: map[string]Summary{},
		}

		objs := s.ccache.List(gvk)
		if objs == nil {
			// No cluster-cache informer for this GVK: the resource does not
			// advertise the watch verb so validSchema() skipped it.  Fall back
			// to the schema's own Store so we still return an accurate count.
			if ic, err := s.listFromStore(apiOp, schema); err == nil {
				counts[schema.ID] = ic
			} else {
				logrus.Debugf("counts: store fallback failed for %s: %v", schema.ID, err)
			}
			continue
		}

		all := access.Grants("list", "*", "*")

		for _, obj := range objs {
			name, ns, revision, summary, ok := getInfo(obj, schema)
			if !ok {
				continue
			}

			if !all && !access.Grants("list", ns, name) && !access.Grants("get", ns, name) {
				continue
			}

			if revision > rev {
				rev = revision
			}

			itemCount = addCounts(itemCount, ns, summary)
		}

		itemCount.Revision = rev
		counts[schema.ID] = itemCount
	}

	return Count{
		ID:     "count",
		Counts: counts,
	}
}
