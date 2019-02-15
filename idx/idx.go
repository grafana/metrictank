// The idx package provides a metadata index for metrics

package idx

import (
	"time"

	"github.com/raintank/schema"
)

var OrgIdPublic = uint32(0)

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
	IrId     uint16 // index in mdata.indexrules (not persisted)
	LastSave uint32 // last time the metricDefinition was saved to a backend store (cassandra)
}

type MetaTagRecord struct {
	MetaTags []string
	Queries  []string
	ID       uint32
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

	// Update updates an existing archive, if found.
	// It returns whether it was found, and - if so - the (updated) existing archive and its old partition
	Update(point schema.MetricPoint, partition int32) (Archive, int32, bool)

	// AddOrUpdate makes sure a metric is known in the index,
	// and should be called for every received metric.
	AddOrUpdate(mkey schema.MKey, data *schema.MetricData, partition int32) (Archive, int32, bool)

	// Get returns the archive for the requested id.
	Get(key schema.MKey) (Archive, bool)

	// GetPath returns the archives under the given path.
	GetPath(orgId uint32, path string) []Archive

	// Delete deletes items from the index
	// If the pattern matches a branch node, then
	// all leaf nodes on that branch are deleted. So if the pattern is
	// "*", all items in the index are deleted.
	// It returns a copy of all of the Archives deleted.
	Delete(orgId uint32, pattern string) ([]Archive, error)

	// Find searches the index for matching nodes.
	// * orgId describes the org to search in (public data in orgIdPublic is automatically included)
	// * pattern is handled like graphite does. see https://graphite.readthedocs.io/en/latest/render_api.html#paths-and-wildcards
	// * from is a unix timestamp. series not updated since then are excluded.
	Find(orgId uint32, pattern string, from int64) ([]Node, error)

	// List returns all Archives for the passed OrgId and the public orgId
	List(orgId uint32) []Archive

	// Prune deletes all metrics that haven't been seen since the given timestamp.
	// It returns all Archives deleted and any error encountered.
	Prune(oldest time.Time) ([]Archive, error)

	// FindByTag takes a list of expressions in the format key<operator>value.
	// The allowed operators are: =, !=, =~, !=~.
	// It returns a slice of Node structs that match the given conditions, the
	// conditions are logically AND-ed.
	// If the third argument is > 0 then the results will be filtered and only those
	// where the LastUpdate time is >= from will be returned as results.
	// The returned results are not deduplicated and in certain cases it is possible
	// that duplicate entries will be returned.
	FindByTag(orgId uint32, expressions []string, from int64) ([]Node, error)

	// Tags returns a list of all tag keys associated with the metrics of a given
	// organization. The return values are filtered by the regex in the second parameter.
	// If the third parameter is >0 then only metrics will be accounted of which the
	// LastUpdate time is >= the given value.
	Tags(orgId uint32, filter string, from int64) ([]string, error)

	// FindTags generates a list of possible tags that could complete a
	// given prefix. It also accepts additional tag conditions to further narrow
	// down the result set in the format of graphite's tag queries
	FindTags(orgId uint32, prefix string, expressions []string, from int64, limit uint) ([]string, error)

	// FindTagValues generates a list of possible values that could
	// complete a given value prefix. It requires a tag to be specified and only values
	// of the given tag will be returned. It also accepts additional conditions to
	// further narrow down the result set in the format of graphite's tag queries
	FindTagValues(orgId uint32, tag string, prefix string, expressions []string, from int64, limit uint) ([]string, error)

	// TagDetails returns a list of all values associated with a given tag key in the
	// given org. The occurrences of each value is counted and the count is referred to by
	// the metric names in the returned map.
	// If the third parameter is not "" it will be used as a regular expression to filter
	// the values before accounting for them.
	// If the fourth parameter is > 0 then only those metrics of which the LastUpdate
	// time is >= the from timestamp will be included.
	TagDetails(orgId uint32, key string, filter string, from int64) (map[string]uint64, error)

	// DeleteTagged deletes the specified series from the tag index and also the
	// DefById index.
	DeleteTagged(orgId uint32, paths []string) ([]Archive, error)

	// MetaTagRecordUpsert inserts or updates a meta record, depending on whether
	// it already exists or is new. The identity of a record is determined by its
	// queries. If the set of queries in the given record already exists in another
	// record, then the existing record will be updated, otherwise a new one gets
	// created.
	// The return values are:
	// 1) The relevant meta record as it is after this operation
	// 2) A bool that is true if the record has been created, or false if updated
	// 3) An error which is nil if no error has occurred
	MetaTagRecordUpsert(orgId uint32, record MetaTagRecord) (MetaTagRecord, bool, error)

	// MetaTagRecordList takes an org id and returns the list of all meta tag records
	// of that given org.
	MetaTagRecordList(orgId uint32) []MetaTagRecord
}
