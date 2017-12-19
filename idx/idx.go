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

// The MetricIndex interface supports Graphite style queries.
// Note:
// * metrictank is a multi-tenant system where different orgs cannot see each
//   other's data, and any given metric name may appear multiple times,
//   under different organisations
//
// * Each metric path can be mapped to multiple metricDefinitions in the case that
//   fields other then the Name vary.  The most common occurrence of this is when
//   the Interval at which the metric is being collected has changed.
type MetricIndex interface {
	// Init initializes the index at startup and
	// blocks until the index is ready for use.
	Init() error

	// Stop shuts down the index.
	Stop()

	// AddOrUpdate makes sure a metric is known in the index,
	// and should be called for every received metric.
	AddOrUpdate(*schema.MetricData, int32) Archive

	// Get returns the archive for the requested id.
	Get(string) (Archive, bool)

	// GetPath returns the archives under the given path.
	GetPath(int, string) []Archive

	// Delete deletes items from the index for the given org and query.
	// If the pattern matches a branch node, then
	// all leaf nodes on that branch are deleted. So if the pattern is
	// "*", all items in the index are deleted.
	// It returns a copy of all of the Archives deleted.
	Delete(int, string) ([]Archive, error)

	// Find searches the index.  The method is passed an OrgId, a query
	// pattern and a unix timestamp. Searches should return all nodes that match for
	// the given OrgId and OrgId -1.  The pattern should be handled in the same way
	// Graphite would. see https://graphite.readthedocs.io/en/latest/render_api.html#paths-and-wildcards
	// And the unix stimestamp is used to ignore series that have been stale since
	// the timestamp.
	Find(int, string, int64) ([]Node, error)

	// List returns all Archives for the passed OrgId, or for all organisations if -1 is provided.
	List(int) []Archive

	// Prune deletes all metrics from the index for the passed org where
	// the last time the metric was seen is older then the passed timestamp. If the org
	// passed is -1, then the all orgs should be examined for stale metrics to be deleted.
	// It returns all Archives deleted and any error encountered.
	Prune(int, time.Time) ([]Archive, error)

	// FindByTag takes a list of expressions in the format key<operator>value.
	// The allowed operators are: =, !=, =~, !=~.
	// It returns a slice of Node structs that match the given conditions, the
	// conditions are logically AND-ed.
	// If the third argument is > 0 then the results will be filtered and only those
	// where the LastUpdate time is >= from will be returned as results.
	// The returned results are not deduplicated and in certain cases it is possible
	// that duplicate entries will be returned.
	FindByTag(int, []string, int64) ([]Node, error)

	// Tags returns a list of all tag keys associated with the metrics of a given
	// organization. The return values are filtered by the regex in the second parameter.
	// If the third parameter is >0 then only metrics will be accounted of which the
	// LastUpdate time is >= the given value.
	Tags(int, string, int64) ([]string, error)

	// FindTags generates a list of possible tags that could complete a
	// given prefix. It also accepts additional tag conditions to further narrow
	// down the result set in the format of graphite's tag queries
	FindTags(int, string, []string, int64, uint) ([]string, error)

	// FindTagValues generates a list of possible values that could
	// complete a given value prefix. It requires a tag to be specified and only values
	// of the given tag will be returned. It also accepts additional conditions to
	// further narrow down the result set in the format of graphite's tag queries
	FindTagValues(int, string, string, []string, int64, uint) ([]string, error)

	// TagDetails returns a list of all values associated with a given tag key in the
	// given org. The occurences of each value is counted and the count is referred to by
	// the metric names in the returned map.
	// If the third parameter is not "" it will be used as a regular expression to filter
	// the values before accounting for them.
	// If the fourth parameter is > 0 then only those metrics of which the LastUpdate
	// time is >= the from timestamp will be included.
	TagDetails(int, string, string, int64) (map[string]uint64, error)
}
