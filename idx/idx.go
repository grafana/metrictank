// The idx package provides a metadata index for metrics

package idx

import (
	"regexp"
	"time"

	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/raintank/schema"
	goi "github.com/robert-milan/go-object-interning"
)

var OrgIdPublic = uint32(0)

// IdxIntern is a pointer into the object interning layer for the index
//
// Default config does not use compression
var IdxIntern = goi.NewObjectIntern(goi.NewConfig())

//msgp:ignore Md5Hash

// Md5Hash is a structure for more compactly storing an md5 hash than using a string
type Md5Hash struct {
	Upper uint64
	Lower uint64
}

//go:generate msgp

type Node struct {
	Path        string
	Leaf        bool
	Defs        []Archive
	HasChildren bool
}

type Archive struct {
	*MetricDefinition
	SchemaId uint16 // index in mdata.schemas (not persisted)
	AggId    uint16 // index in mdata.aggregations (not persisted)
	IrId     uint16 // index in mdata.indexrules (not persisted)
	LastSave uint32 // last time the metricDefinition was saved to a backend store (cassandra)
}

// used primarily by tests, for convenience
func NewArchiveBare(name string) Archive {
	arc := Archive{}
	arc.MetricDefinition = new(MetricDefinition)
	err := arc.MetricDefinition.SetMetricName(name)
	if err != nil {
		return Archive{}
	}
	return arc
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
	Update(point schema.MetricPoint, partition int32) (*Archive, int32, bool)

	// AddOrUpdate makes sure a metric is known in the index,
	// and should be called for every received metric.
	AddOrUpdate(mkey schema.MKey, data *schema.MetricData, partition int32) (*Archive, int32, bool)

	// Get returns the archive for the requested id.
	Get(key schema.MKey) (Archive, bool)

	// GetPath returns the archives under the given path.
	GetPath(orgId uint32, path string) []Archive

	// Delete deletes items from the index
	// If the pattern matches a branch node, then
	// all leaf nodes on that branch are deleted. So if the pattern is
	// "*", all items in the index are deleted.
	// It returns the number of Archives that were deleted.
	Delete(orgId uint32, pattern string) (int, error)

	// DeletePersistent deletes items from the index
	// It returns the deleted items in a []idx.Archive
	DeletePersistent(orgId uint32, pattern string) ([]Archive, error)

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

	// FindByTag takes a query object and executes the query on the index. The query
	// is composed of one or many query expressions, plus a "from condition" defining
	// after which time a metric must have received its last data point in order to
	// be returned in the result.
	// The returned results are not deduplicated and in certain cases it is possible
	// that duplicate entries will be returned.
	FindByTag(orgId uint32, query tagquery.Query) []Node

	// Tags returns a list of all tag keys associated with the metrics of a given
	// organization. The return values are filtered by the regex in the second parameter.
	// If the third parameter is >0 then only tags will be returned of which the
	// LastUpdate of at least one metric with that tag is >= of the given from value.
	Tags(orgId uint32, filter *regexp.Regexp, from int64) []string

	// FindTags generates a list of possible tags that could complete a
	// given prefix. It only supports simple queries by prefix and from,
	// without any further conditions. But its faster than the alternative
	// FindTagsWithQuery()
	FindTags(orgId uint32, prefix string, from int64, limit uint) []string

	// FindTagsWithQuery generates a list of possible tags that could complete
	// a given prefix. It runs a full query on the index, so it allows the
	// user to narrow down the result by specifying additional expressions,
	// but if the query isn't necessary it is recommended to use FindTags()
	// because it is faster
	FindTagsWithQuery(orgId uint32, prefix string, query tagquery.Query, limit uint) []string

	// FindTagValues generates a list of possible values that could complete
	// a given value prefix. It requires a tag to be specified and only values
	// of the given tag will be returned. It also allows the caller to further
	// narrow down the results by specifying a from value, if from >=0 then
	// only values will be returned of which at least one metric has received
	// a datapoint since or at "from". The "limit" limits the result set to a
	// specified length, since the results are sorted before being sliced it
	// can be relied on that always the first "limit" entries of the result
	// set will be returned.
	FindTagValues(orgId uint32, tag, prefix string, from int64, limit uint) []string

	// FindTagValuesWithQuery does the same thing as FindTagValues, but additionally it
	// allows the caller to pass a tag query which is used to further narrow down the
	// result set. If the tag query is not necessary, it is recommended to use
	// FindTagValues() because it is faster
	FindTagValuesWithQuery(orgId uint32, tag, prefix string, query tagquery.Query, limit uint) []string

	// TagDetails returns a list of all values associated with a given tag key in the
	// given org. The occurrences of each value is counted and the count is referred to by
	// the metric names in the returned map.
	// If the third parameter is not nil it will be used to filter the values before
	// accounting for them.
	// If the fourth parameter is > 0 then only those metrics of which the LastUpdate
	// time is >= the from timestamp will be included.
	TagDetails(orgId uint32, key string, filter *regexp.Regexp, from int64) map[string]uint64

	// DeleteTagged deletes the series returned by the given query from the tag index
	// and also the DefById index.
	DeleteTagged(orgId uint32, query tagquery.Query) []Archive

	// MetaTagRecordUpsert inserts, updates or deletes a meta record, depending on
	// whether it already exists or is new. The identity of a record is determined
	// by its queries.
	// If the set of queries in the given record already exists in another record,
	// then the existing record will be updated, otherwise a new one gets created.
	// If an existing record is updated with one that has no meta tags
	// associated, then this operation results in the deletion of the meta record
	// because a meta record has no effect without meta tags.
	// The return values are:
	// 1) The relevant meta record as it is after this operation
	// 2) A bool that is true if the record has been created, or false if updated
	// 3) An error which is nil if no error has occurred
	MetaTagRecordUpsert(orgId uint32, record tagquery.MetaTagRecord) (tagquery.MetaTagRecord, bool, error)

	// MetaTagRecordList takes an org id and returns the list of all meta tag records
	// of that given org.
	MetaTagRecordList(orgId uint32) []tagquery.MetaTagRecord
}

// InternReleaseMetricDefinition releases all previously acquired strings
// into the interning layer so that their reference count can be
// decreased by 1 and they can eventually be deleted
func InternReleaseMetricDefinition(md MetricDefinition) {
	IdxIntern.DeleteBatch(md.Name.Nodes())
	for i := range md.Tags.KeyValues {
		IdxIntern.DeleteBatch([]uintptr{md.Tags.KeyValues[i].Key, md.Tags.KeyValues[i].Value})
	}
	IdxIntern.DeleteByString(md.Unit)
}

func InternIncMetricDefinitionRefCounts(md MetricDefinition) {
	IdxIntern.IncRefCntBatch(md.Name.Nodes())
	for i := range md.Tags.KeyValues {
		IdxIntern.IncRefCntBatch([]uintptr{md.Tags.KeyValues[i].Key, md.Tags.KeyValues[i].Value})
	}
	IdxIntern.IncRefCntByString(md.Unit)
}
