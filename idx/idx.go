// The idx package provides a metadata index for metrics

package idx

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	schema "gopkg.in/raintank/schema.v1"
)

var (
	BothBranchAndLeaf  = errors.New("node can't be both branch and leaf")
	BranchUnderLeaf    = errors.New("can't add branch under leaf")
	errInvalidQuery    = errors.New("invalid query")
	errInvalidIdString = errors.New("invalid ID string")
)

//go:generate msgp
type Node struct {
	Path        string
	Leaf        bool
	Defs        []Archive
	HasChildren bool
}

type Archive struct {
	schema.MetricDefinition
	SchemaId uint16 // index in mdata.schemas (not persisted)
	AggId    uint16 // index in mdata.aggregations (not persisted)
	LastSave uint32 // last time the metricDefinition was saved to a backend store (cassandra)
}

type MetricID struct {
	org int
	key [16]byte
}

func NewMetricIDFromString(s string) (MetricID, error) {
	id := MetricID{}
	err := id.FromString(s)
	return id, err
}

func (id *MetricID) FromString(s string) error {
	splits := strings.Split(s, ".")
	if len(splits) != 2 || len(splits[1]) != 32 {
		return errInvalidIdString
	}

	var err error
	id.org, err = strconv.Atoi(splits[0])
	if err != nil {
		return err
	}

	dst := make([]byte, 16)
	n, err := hex.Decode(dst, []byte(splits[1]))
	if n != 16 {
		return errInvalidIdString
	}
	copy(id.key[:], dst)
	return nil
}

func (id *MetricID) String() string {
	return fmt.Sprintf("%d.%x", id.org, id.key)
}

// used primarily by tests, for convenience
func NewArchiveBare(name string) Archive {
	return Archive{
		MetricDefinition: schema.MetricDefinition{
			Name: name,
		},
	}
}

/*
Currently the index is solely used for supporting Graphite style queries.
So, the index only needs to be able to search by a pattern that matches the
MetricDefinition.Name field. In future we plan to extend the searching
capabilities to include the other fields in the definition.

Note:

* metrictank is a multi-tenant system where different orgs cannot see each
  other's data

* any given metric may appear multiple times, under different organisations

* Each metric path can be mapped to multiple metricDefinitions in the case that
  fields other then the Name vary.  The most common occurrence of this is when
  the Interval at which the metric is being collected has changed.

Interface

* Init()
  This is the initialization step performed at startup. This method should
  block until the index is ready to handle searches.

* Stop():
 This will be called when metrictank is shutting down.

* AddOrUpdate(*schema.MetricData, int32) Archive:
  Every metric received will result in a call to this method to ensure the
  metric has been added to the index. The method is passed the metricData
  payload and the partition id of the metric

* Get(string) (Archive, bool):
  This method should return the MetricDefintion with the passed Id.

* GetPath(string) (Archive, bool) []Archive:
  This method should return the archives under the given path

* List(int) []Archive:
  This method should return all MetricDefinitions for the passed OrgId.  If the
  passed OrgId is "-1", then all metricDefinitions across all organisations
  should be returned.

* Find(int, string, int64) ([]Node, error):
  This method provides searches.  The method is passed an OrgId, a query
  pattern and a unix timestamp. Searches should return all nodes that match for
  the given OrgId and OrgId -1.  The pattern should be handled in the same way
  Graphite would. see https://graphite.readthedocs.io/en/latest/render_api.html#paths-and-wildcards
  And the unix stimestamp is used to ignore series that have been stale since
  the timestamp.

* Delete(int, string) ([]Archive, error):
  This method is used for deleting items from the index. The method is passed
  an OrgId and a query pattern.  If the pattern matches a branch node, then
  all leaf nodes on that branch should also be deleted. So if the pattern is
  "*", all items in the index should be deleted.  A copy of all of the
  metricDefinitions deleted are returned.

* Prune(int, time.Time) ([]Archive, error):
  This method should delete all metrics from the index for the passed org where
  the last time the metric was seen is older then the passed timestamp. If the org
  passed is -1, then the all orgs should be examined for stale metrics to be deleted.
  The method returns a list of the metricDefinitions deleted from the index and any
  error encountered.

* TagList(int) []string:
  This method returns a list of all tag keys associated with the metrics of a given
  organization.

* Tag(int, string, int64) map[string]uint32:
  This method returns a list of all values associated with a given tag key in the
  given org. The occurences of each value is counted and the count is referred to by
  the series ids in the returned map. If the third parameter is > 0 then the metrics
  will be filtered and only those of which the LastUpdate time is >= the from
  timestamp will be considered while the others are being ignored.

* FindByTag(int, []string, int64) ([]string, error):
  This method takes a list of expressions in the format key<operator>value.
  The allowed operators are: =, !=, =~, !=~.
  It returns a slice of IDs that match the given conditions, the conditions are
  logically AND-ed. If the third argument is > 0 then the results will be filtered
  and only those where the LastUpdate time is >= from will be returned as results.
*/

type MetricIndex interface {
	Init() error
	Stop()
	AddOrUpdate(*schema.MetricData, int32) Archive
	Get(string) (Archive, bool)
	GetPath(int, string) []Archive
	Delete(int, string) ([]Archive, error)
	Find(int, string, int64) ([]Node, error)
	List(int) []Archive
	Prune(int, time.Time) ([]Archive, error)
	TagList(int) []string
	Tag(int, string, int64) map[string]uint32
	FindByTag(int, []string, int64) (map[MetricID]struct{}, error)
}
