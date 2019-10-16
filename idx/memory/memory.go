package memory

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/errors"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/stats"
	log "github.com/sirupsen/logrus"
)

var (
	// metric idx.memory.ops.update is the number of updates to the memory idx
	statUpdate = stats.NewCounter32("idx.memory.ops.update")
	// metric idx.memory.ops.add is the number of additions to the memory idx
	statAdd = stats.NewCounter32("idx.memory.ops.add")
	// metric idx.memory.add is the duration of a (successful) add of a metric to the memory idx
	statAddDuration = stats.NewLatencyHistogram15s32("idx.memory.add")
	// metric idx.memory.update is the duration of (successful) update of a metric to the memory idx
	statUpdateDuration = stats.NewLatencyHistogram15s32("idx.memory.update")
	// metric idx.memory.get is the duration of a get of one metric in the memory idx
	statGetDuration = stats.NewLatencyHistogram15s32("idx.memory.get")
	// metric idx.memory.list is the duration of memory idx listings
	statListDuration = stats.NewLatencyHistogram15s32("idx.memory.list")
	// metric idx.memory.find is the duration of memory idx find
	statFindDuration = stats.NewLatencyHistogram15s32("idx.memory.find")
	// metric idx.memory.delete is the duration of a delete of one or more metrics from the memory idx
	statDeleteDuration = stats.NewLatencyHistogram15s32("idx.memory.delete")
	// metric idx.memory.prune is the duration of successful memory idx prunes
	statPruneDuration = stats.NewLatencyHistogram15s32("idx.memory.prune")

	// metric idx.memory.filtered is number of series that have been excluded from responses due to their lastUpdate property
	statFiltered = stats.NewCounter32("idx.memory.filtered")

	// metric idx.metrics_active is the number of currently known metrics in the index
	statMetricsActive = stats.NewGauge32("idx.metrics_active")

	Enabled                      bool
	maxPruneLockTime             = time.Millisecond * 100
	maxPruneLockTimeStr          string
	TagSupport                   bool
	TagQueryWorkers              int // number of workers to spin up when evaluation tag expressions
	indexRulesFile               string
	IndexRules                   conf.IndexRules
	Partitioned                  bool
	findCacheSize                = 1000
	findCacheInvalidateQueueSize = 200
	findCacheInvalidateMaxSize   = 100
	findCacheInvalidateMaxWait   = 5 * time.Second
	findCacheBackoffTime         = time.Minute
	writeQueueEnabled            = false
	writeQueueDelay              = 30 * time.Second
	writeMaxBatchSize            = 5000
	matchCacheSize               = 1000
	enrichmentCacheSize          = 10000
	MetaTagSupport               = false
)

func ConfigSetup() *flag.FlagSet {
	memoryIdx := flag.NewFlagSet("memory-idx", flag.ExitOnError)
	memoryIdx.BoolVar(&Enabled, "enabled", false, "")
	memoryIdx.BoolVar(&TagSupport, "tag-support", false, "enables/disables querying based on tags")
	memoryIdx.BoolVar(&Partitioned, "partitioned", false, "use separate indexes per partition. experimental feature")
	memoryIdx.IntVar(&TagQueryWorkers, "tag-query-workers", 50, "number of workers to spin up to evaluate tag queries")
	memoryIdx.IntVar(&findCacheSize, "find-cache-size", 1000, "number of find expressions to cache (per org). 0 disables cache")
	memoryIdx.IntVar(&findCacheInvalidateQueueSize, "find-cache-invalidate-queue-size", 200, "size of queue for invalidating findCache entries")
	memoryIdx.IntVar(&findCacheInvalidateMaxSize, "find-cache-invalidate-max-size", 100, "max amount of invalidations to queue up in one batch")
	memoryIdx.BoolVar(&writeQueueEnabled, "write-queue-enabled", false, "enable buffering new metricDefinitions and writing them to the index in batches")
	memoryIdx.DurationVar(&writeQueueDelay, "write-queue-delay", 30*time.Second, "maximum delay between flushing buffered metric writes to the index")
	memoryIdx.IntVar(&writeMaxBatchSize, "write-max-batch-size", 5000, "maximum number of metricDefinitions that can be added to the index in a single batch")
	memoryIdx.DurationVar(&findCacheInvalidateMaxWait, "find-cache-invalidate-max-wait", 5*time.Second, "max duration to wait building up a batch to invalidate")
	memoryIdx.DurationVar(&findCacheBackoffTime, "find-cache-backoff-time", time.Minute, "amount of time to disable the findCache when the invalidate queue fills up.")
	memoryIdx.StringVar(&indexRulesFile, "rules-file", "/etc/metrictank/index-rules.conf", "path to index-rules.conf file")
	memoryIdx.StringVar(&maxPruneLockTimeStr, "max-prune-lock-time", "100ms", "Maximum duration each second a prune job can lock the index.")
	memoryIdx.IntVar(&matchCacheSize, "match-cache-size", 1000, "size of regular expression cache in tag query evaluation")
	memoryIdx.IntVar(&enrichmentCacheSize, "enrichment-cache-size", 10000, "size of the meta tag enrichment cache")
	memoryIdx.BoolVar(&MetaTagSupport, "meta-tag-support", false, "enables/disables querying based on meta tags which get defined via meta tag rules")
	globalconf.Register("memory-idx", memoryIdx, flag.ExitOnError)
	return memoryIdx
}

func ConfigProcess() {
	var err error
	maxPruneLockTime, err = time.ParseDuration(maxPruneLockTimeStr)
	if err != nil {
		log.Fatalf("could not parse max-prune-lock-time %q: %s", maxPruneLockTimeStr, err)
	}
	if maxPruneLockTime > time.Second {
		log.Fatalf("invalid max-prune-lock-time of %s. Must be <= 1 second", maxPruneLockTimeStr)
	}
	// read index-rules.conf
	IndexRules, err = conf.ReadIndexRules(indexRulesFile)
	if os.IsNotExist(err) {
		log.Infof("Index-rules.conf file %s does not exist; using defaults", indexRulesFile)
		IndexRules = conf.NewIndexRules()
	} else if err != nil {
		log.Fatalf("can't read index-rules file %q: %s", indexRulesFile, err.Error())
	}

	if findCacheInvalidateMaxSize >= findCacheInvalidateQueueSize {
		log.Fatal("find-cache-invalidate-max-size should be smaller than find-cache-invalidate-queue-size")
	}

	tagquery.MetaTagSupport = MetaTagSupport
	tagquery.MatchCacheSize = matchCacheSize
}

// interface implemented by both UnpartitionedMemoryIdx and PartitionedMemoryIdx
// this is needed to support unit tests.
type MemoryIndex interface {
	idx.MetricIndex
	LoadPartition(int32, []schema.MetricDefinition) int
	UpdateArchiveLastSave(schema.MKey, int32, uint32)
	add(*idx.Archive)
	idsByTagQuery(uint32, TagQueryContext) chan schema.MKey
	PurgeFindCache()
	ForceInvalidationFindCache()
}

func New() MemoryIndex {
	if Partitioned {
		return NewPartitionedMemoryIdx()
	}
	return NewUnpartitionedMemoryIdx()
}

type Tree struct {
	Items map[string]*Node // key is the full path of the node.
}

type IdSet map[schema.MKey]struct{} // set of ids

func (ids IdSet) String() string {
	var res string
	for id := range ids {
		if len(res) > 0 {
			res += " "
		}
		res += id.String()
	}
	return res

}

type TagValues map[string]IdSet    // value -> set of ids
type TagIndex map[string]TagValues // key -> list of values

func (t *TagIndex) addTagId(name, value string, id schema.MKey) {
	ti := *t
	if _, ok := ti[name]; !ok {
		ti[name] = make(TagValues)
	}
	if _, ok := ti[name][value]; !ok {
		ti[name][value] = make(IdSet)
	}
	ti[name][value][id] = struct{}{}
}

func (t *TagIndex) delTagId(name, value string, id schema.MKey) {
	ti := *t

	delete(ti[name][value], id)

	if len(ti[name][value]) == 0 {
		delete(ti[name], value)
		if len(ti[name]) == 0 {
			delete(ti, name)
		}
	}
}

// org id -> nameWithTags -> Set of references to schema.MetricDefinition
// nameWithTags is the name plus all tags in the <name>;<tag>=<value>... format.
type defByTagSet map[uint32]map[string]map[*schema.MetricDefinition]struct{}

func (t TagIndex) idHasTag(id schema.MKey, tag, value string) bool {
	_, ok := t[tag][value][id]
	return ok
}

func (defs defByTagSet) add(def *schema.MetricDefinition) {
	var orgDefs map[string]map[*schema.MetricDefinition]struct{}
	var ok bool
	if orgDefs, ok = defs[def.OrgId]; !ok {
		orgDefs = make(map[string]map[*schema.MetricDefinition]struct{})
		defs[def.OrgId] = orgDefs
	}

	fullName := def.NameWithTags()
	if _, ok = orgDefs[fullName]; !ok {
		orgDefs[fullName] = make(map[*schema.MetricDefinition]struct{}, 1)
	}
	orgDefs[fullName][def] = struct{}{}
}

func (defs defByTagSet) del(def *schema.MetricDefinition) {
	var orgDefs map[string]map[*schema.MetricDefinition]struct{}
	var ok bool
	if orgDefs, ok = defs[def.OrgId]; !ok {
		return
	}

	fullName := def.NameWithTags()
	delete(orgDefs[fullName], def)

	if len(orgDefs[fullName]) == 0 {
		delete(orgDefs, fullName)
	}

	if len(orgDefs) == 0 {
		delete(defs, def.OrgId)
	}
}

func (defs defByTagSet) defs(id uint32, fullName string) map[*schema.MetricDefinition]struct{} {
	var orgDefs map[string]map[*schema.MetricDefinition]struct{}
	var ok bool
	if orgDefs, ok = defs[id]; !ok {
		return nil
	}

	return orgDefs[fullName]
}

type Node struct {
	Path     string // branch or NameWithTags for leafs
	Children []string
	Defs     []schema.MKey
}

func (n *Node) HasChildren() bool {
	return len(n.Children) > 0
}

func (n *Node) Leaf() bool {
	return len(n.Defs) > 0
}

func (n *Node) String() string {
	if n.Leaf() {
		return fmt.Sprintf("leaf - %s", n.Path)
	}
	return fmt.Sprintf("branch - %s", n.Path)
}

type UnpartitionedMemoryIdx struct {
	sync.RWMutex

	// used for both hierarchy and tag index, so includes all MDs, with
	// and without tags. It also mixes all orgs into one flat map.
	defById map[schema.MKey]*idx.Archive

	// used by hierarchy index only
	tree map[uint32]*Tree // by orgId

	// used by tag index
	defByTagSet    defByTagSet
	tags           map[uint32]TagIndex        // by orgId
	metaTagIndex   map[uint32]metaTagIndex    // by orgId
	metaTagRecords map[uint32]*metaTagRecords // by orgId

	findCache *FindCache

	writeQueue *WriteQueue
}

func NewUnpartitionedMemoryIdx() *UnpartitionedMemoryIdx {
	m := &UnpartitionedMemoryIdx{
		defById:        make(map[schema.MKey]*idx.Archive),
		defByTagSet:    make(defByTagSet),
		tree:           make(map[uint32]*Tree),
		tags:           make(map[uint32]TagIndex),
		metaTagIndex:   make(map[uint32]metaTagIndex),
		metaTagRecords: make(map[uint32]*metaTagRecords),
	}
	return m
}

func (m *UnpartitionedMemoryIdx) Init() error {
	if findCacheSize > 0 {
		log.Infof("memoryIdx: enabling findCache with with a size of %d items", findCacheSize)
		m.findCache = NewFindCache(findCacheSize, findCacheInvalidateQueueSize, findCacheInvalidateMaxSize, findCacheInvalidateMaxWait, findCacheBackoffTime)
	}
	if writeQueueEnabled {
		m.writeQueue = NewWriteQueue(m, writeQueueDelay, writeMaxBatchSize)
	}
	return nil
}

func (m *UnpartitionedMemoryIdx) Stop() {
	if m.findCache != nil {
		m.findCache.Shutdown()
		m.findCache = nil
	}
	if m.writeQueue != nil {
		m.writeQueue.Stop()
		m.writeQueue = nil
	}
	return
}

// bumpLastUpdate increases lastUpdate.
// note:
// * received point may be older than a previously received point, in which case the previous value was correct
// * someone else may have just concurrently updated lastUpdate to a higher value than what we have, which we should restore
// * by the time we look at the previous value and try to restore it, someone else may have updated it to a higher value
// all these scenarios are unlikely but we should accommodate them anyway.
func bumpLastUpdate(loc *int64, newVal int64) {
	prev := atomic.SwapInt64(loc, newVal)
	for prev > newVal {
		newVal = prev
		prev = atomic.SwapInt64(loc, newVal)
	}
}

// updates the partition and lastUpdate ts in an archive. Returns the previously set partition
func updateExisting(existing *idx.Archive, partition int32, lastUpdate int64, pre time.Time) int32 {
	bumpLastUpdate(&existing.LastUpdate, lastUpdate)

	oldPart := atomic.SwapInt32(&existing.Partition, partition)
	statUpdate.Inc()
	statUpdateDuration.Value(time.Since(pre))
	return oldPart
}

// Update updates an existing archive, if found.
// It returns whether it was found, and - if so - the (updated) existing archive and its old partition
func (m *UnpartitionedMemoryIdx) Update(point schema.MetricPoint, partition int32) (idx.Archive, int32, bool) {
	pre := time.Now()

	m.RLock()
	existing, ok := m.defById[point.MKey]
	m.RUnlock()
	if ok {
		if log.IsLevelEnabled(log.DebugLevel) {
			log.Debugf("memory-idx: metricDef with id %v already in index", point.MKey)
		}

		oldPart := updateExisting(existing, partition, int64(point.Time), pre)
		return CloneArchive(existing), oldPart, true
	}

	if m.writeQueue != nil {
		// if we are using the writeQueue, then the archive for this MKey might be queued
		// and not yet flushed to the index yet.
		existing, ok := m.writeQueue.Get(point.MKey)
		if ok {
			if log.IsLevelEnabled(log.DebugLevel) {
				log.Debugf("memory-idx: metricDef with id %v is in the writeQueue", point.MKey)
			}
			oldPart := updateExisting(existing, partition, int64(point.Time), pre)
			return CloneArchive(existing), oldPart, true
		}

		// we need to do one final check of m.defById, as the writeQueue may have been flushed between
		// when we released m.RLock() and when the call to m.writeQueue.Get() was able to obtain its own lock.
		m.RLock()
		existing, ok = m.defById[point.MKey]
		m.RUnlock()
		if ok {
			if log.IsLevelEnabled(log.DebugLevel) {
				log.Debugf("memory-idx: metricDef with id %v already in index", point.MKey)
			}
			oldPart := updateExisting(existing, partition, int64(point.Time), pre)
			return CloneArchive(existing), oldPart, true
		}
	}

	return idx.Archive{}, 0, false
}

// AddOrUpdate returns the corresponding Archive for the MetricData.
// if it is existing -> updates lastUpdate based on .Time, and partition
// if was new        -> adds new MetricDefinition to index
func (m *UnpartitionedMemoryIdx) AddOrUpdate(mkey schema.MKey, data *schema.MetricData, partition int32) (idx.Archive, int32, bool) {
	pre := time.Now()

	// we only need a lock while reading the m.defById map. All future operations on the archive
	// use sync/atomic to allow concurrent read/writes
	m.RLock()
	existing, ok := m.defById[mkey]
	m.RUnlock()
	if ok {
		if log.IsLevelEnabled(log.DebugLevel) {
			log.Debugf("memory-idx: metricDef with id %s already in the index", mkey)
		}
		oldPart := updateExisting(existing, partition, data.Time, pre)
		return CloneArchive(existing), oldPart, ok
	}
	def := schema.MetricDefinitionFromMetricData(data)
	def.Partition = partition
	archive := createArchive(def)
	if m.writeQueue == nil {
		// writeQueue not enabled, so acquire a wlock and immediately add to the index.
		m.Lock()
		m.add(archive)
		m.Unlock()
		statAddDuration.Value(time.Since(pre))
	} else {
		// push the new archive into the writeQueue.  If there is already an archive in the
		// writeQueue with the same mkey, it will be replaced.
		m.writeQueue.Queue(archive)
	}

	return CloneArchive(archive), 0, false
}

// UpdateArchiveLastSave updates the LastSave timestamp of the archive
func (m *UnpartitionedMemoryIdx) UpdateArchiveLastSave(id schema.MKey, partition int32, lastSave uint32) {
	m.RLock()
	existing, ok := m.defById[id]
	m.RUnlock()
	if ok {
		atomic.StoreUint32(&existing.LastSave, lastSave)
		return
	}
	// The index may not have an entry for the id for the following reasons
	// - the MetricDef has just been deleted from the index
	// - the metricDef is waiting in the writeQueue and hasnt been added yet
	if m.writeQueue != nil {
		// if we are using the writeQueue, then the archive for this MKey might be queued
		// and not yet flushed to the index yet.
		existing, ok = m.writeQueue.Get(id)
		if ok {
			atomic.StoreUint32(&existing.LastSave, lastSave)
			return
		}
		// we need to do one final check of m.defById, as the writeQueue may have been flushed between
		// when we released m.RLock() and when the call to m.writeQueue.Get() was able to obtain its own lock.
		m.RLock()
		existing, ok = m.defById[id]
		m.RUnlock()
		if ok {
			atomic.StoreUint32(&existing.LastSave, lastSave)
			return
		}
	}
}

// MetaTagRecordUpsert inserts or updates a meta record, depending on whether
// it already exists or is new. The identity of a record is determined by its
// queries, if the set of queries in the given record already exists in another
// record, then the existing record will be updated, otherwise a new one gets
// created.
func (m *UnpartitionedMemoryIdx) MetaTagRecordUpsert(orgId uint32, upsertRecord tagquery.MetaTagRecord) error {
	if !TagSupport || !MetaTagSupport {
		log.Warn("memory-idx: received tag/meta-tag query, but that feature is disabled")
		return errors.NewBadRequest("Tag/Meta-Tag support is disabled")
	}

	var mtr *metaTagRecords
	var mti metaTagIndex
	var ok bool

	// expressions need to be sorted because the unique ID of a meta record is
	// its sorted set of expressions
	upsertRecord.Expressions.Sort()

	m.Lock()
	defer m.Unlock()

	if mtr, ok = m.metaTagRecords[orgId]; !ok {
		mtr = newMetaTagRecords()
		m.metaTagRecords[orgId] = mtr
	}

	if mti, ok = m.metaTagIndex[orgId]; !ok {
		mti = make(metaTagIndex)
		m.metaTagIndex[orgId] = mti
	}

	id, record, oldId, oldRecord, err := mtr.upsert(upsertRecord)
	if err != nil {
		return err
	}

	// if this upsert has updated a previously existing record, then we remove its entries
	// from the metaTagIndex before inserting the new ones
	if oldRecord != nil {
		for _, keyValue := range oldRecord.MetaTags {
			mti.deleteRecord(keyValue, oldId)
		}

		for _, keyValue := range record.MetaTags {
			mti.insertRecord(keyValue, id)
		}

		return nil
	}

	for _, keyValue := range record.MetaTags {
		mti.insertRecord(keyValue, id)
	}

	return nil
}

func (m *UnpartitionedMemoryIdx) MetaTagRecordSwap(orgId uint32, records []tagquery.MetaTagRecord) error {
	if !TagSupport || !MetaTagSupport {
		log.Warn("memory-idx: received a tag/meta-tag query, but that feature is disabled")
		return errors.NewBadRequest("Tag/Meta-Tag support is disabled")
	}

	newMtr := newMetaTagRecords()
	newMti := make(metaTagIndex)

	for _, record := range records {
		record.Expressions.Sort()

		recordId, _, _, _, err := newMtr.upsert(record)
		if err != nil {
			return err
		}

		for _, keyValue := range record.MetaTags {
			newMti.insertRecord(keyValue, recordId)
		}
	}

	m.Lock()
	defer m.Unlock()

	m.metaTagRecords[orgId] = newMtr
	m.metaTagIndex[orgId] = newMti

	return nil
}

func (m *UnpartitionedMemoryIdx) MetaTagRecordList(orgId uint32) []tagquery.MetaTagRecord {
	var res []tagquery.MetaTagRecord

	m.RLock()
	defer m.RUnlock()

	mtr, ok := m.metaTagRecords[orgId]
	if !ok {
		return res
	}

	res = make([]tagquery.MetaTagRecord, len(mtr.records))
	i := 0
	for _, record := range mtr.records {
		res[i] = record
		i++
	}

	return res
}

// indexTags reads the tags of a given metric definition and creates the
// corresponding tag index entries to refer to it. It assumes a lock is
// already held.
func (m *UnpartitionedMemoryIdx) indexTags(def *schema.MetricDefinition) {
	tags, ok := m.tags[def.OrgId]
	if !ok {
		tags = make(TagIndex)
		m.tags[def.OrgId] = tags
	}

	for _, tag := range def.Tags {
		tagSplits := strings.SplitN(tag, "=", 2)
		if len(tagSplits) < 2 {
			// should never happen because every tag in the index
			// must have a valid format
			invalidTag.Inc()
			log.Errorf("memory-idx: Tag %q of id %q has an invalid format", tag, def.Id)
			continue
		}

		tagName := tagSplits[0]
		tagValue := tagSplits[1]
		tags.addTagId(tagName, tagValue, def.Id)
	}
	tags.addTagId("name", def.NameSanitizedAsTagValue(), def.Id)

	m.defByTagSet.add(def)
}

// deindexTags takes a given metric definition and removes all references
// to it from the tag index. It assumes a lock is already held.
// a return value of "false" means there was an error and the deindexing was
// unsuccessful, "true" means the indexing was at least partially or completely
// successful
func (m *UnpartitionedMemoryIdx) deindexTags(tags TagIndex, def *schema.MetricDefinition) bool {
	for _, tag := range def.Tags {
		tagSplits := strings.SplitN(tag, "=", 2)
		if len(tagSplits) < 2 {
			// should never happen because every tag in the index
			// must have a valid format
			invalidTag.Inc()
			log.Errorf("memory-idx: Tag %q of id %q has an invalid format", tag, def.Id)
			continue
		}

		tagName := tagSplits[0]
		tagValue := tagSplits[1]
		tags.delTagId(tagName, tagValue, def.Id)
	}

	tags.delTagId("name", def.NameSanitizedAsTagValue(), def.Id)

	m.defByTagSet.del(def)

	return true
}

// Used to rebuild the index from an existing set of metricDefinitions for a specific paritition.
func (m *UnpartitionedMemoryIdx) LoadPartition(partition int32, defs []schema.MetricDefinition) int {
	// UnpartitionedMemoryIdx isnt partitioned, so just ignore the partition passed and call Load()
	return m.Load(defs)
}

// Used to rebuild the index from an existing set of metricDefinitions.
func (m *UnpartitionedMemoryIdx) Load(defs []schema.MetricDefinition) int {
	m.Lock()
	defer m.Unlock()
	var pre time.Time
	var num int
	for i := range defs {
		def := &defs[i]
		pre = time.Now()
		if _, ok := m.defById[def.Id]; ok {
			continue
		}

		m.add(createArchive(def))

		// as we are loading the metricDefs from a persistent store, set the lastSave
		// to the lastUpdate timestamp.  This won't exactly match the true lastSave Timstamp,
		// but it will be close enough and it will always be true that the lastSave was at
		// or after this time.  For metrics that are sent at or close to real time (the typical
		// use case), then the value will be within a couple of seconds of the true lastSave.
		m.defById[def.Id].LastSave = uint32(def.LastUpdate)
		num++
		statAddDuration.Value(time.Since(pre))
	}
	return num
}

func createArchive(def *schema.MetricDefinition) *idx.Archive {
	path := def.NameWithTags()
	schemaId, _ := mdata.MatchSchema(path, def.Interval)
	aggId, _ := mdata.MatchAgg(path)
	irId, _ := IndexRules.Match(path)

	return &idx.Archive{
		MetricDefinition: *def,
		SchemaId:         schemaId,
		AggId:            aggId,
		IrId:             irId,
	}
}

func (m *UnpartitionedMemoryIdx) add(archive *idx.Archive) {
	// there is a race condition that can lead to an archive being added
	// to the writeQueue just after a queued copy of the archive was flushed.
	// If that happens, we just do an update lastUpdate instead
	if existing, ok := m.defById[archive.Id]; ok {
		// We deliberately dont update existing.Partition as any change
		// cant be passed back to the caller (cassandraIdx,BigtableIdx).
		// If the partition has changed, then the next datapoint will update
		// the partition and notify the caller of the change.
		bumpLastUpdate(&existing.LastUpdate, archive.LastUpdate)
		return
	}

	statMetricsActive.Inc()

	def := &archive.MetricDefinition
	path := def.NameWithTags()

	if TagSupport {
		// Even if there are no tags, index at least "name". It's important to use the definition
		// in the archive pointer that we add to defById, because the pointers must reference the
		// same underlying object in m.defById and m.defByTagSet
		m.indexTags(def)

		if len(def.Tags) > 0 {
			if _, ok := m.defById[def.Id]; !ok {
				m.defById[def.Id] = archive
				statAdd.Inc()
				log.Debugf("memory-idx: adding %s to DefById", path)
			}
			return
		}
	}

	if m.findCache != nil {
		defer func() {
			go m.findCache.InvalidateFor(def.OrgId, path)
		}()
	}

	//first check to see if a tree has been created for this OrgId
	tree, ok := m.tree[def.OrgId]
	if !ok || len(tree.Items) == 0 {
		log.Debugf("memory-idx: first metricDef seen for orgId %d", def.OrgId)
		root := &Node{
			Path:     "",
			Children: make([]string, 0),
			Defs:     make([]schema.MKey, 0),
		}
		m.tree[def.OrgId] = &Tree{
			Items: map[string]*Node{"": root},
		}
		tree = m.tree[def.OrgId]
	} else {
		// now see if there is an existing branch or leaf with the same path.
		// An existing leaf is possible if there are multiple metricDefs for the same path due
		// to different tags or interval
		if node, ok := tree.Items[path]; ok {
			log.Debugf("memory-idx: existing index entry for %s. Adding %s to Defs list", path, def.Id)
			node.Defs = append(node.Defs, def.Id)
			m.defById[def.Id] = archive
			statAdd.Inc()
			return
		}
	}

	pos := strings.LastIndex(path, ".")

	// now walk backwards through the node path to find the first branch which exists that
	// this path extends.
	prevPos := len(path)
	for pos != -1 {
		branch := path[:pos]
		prevNode := path[pos+1 : prevPos]
		if n, ok := tree.Items[branch]; ok {
			log.Debugf("memory-idx: adding %s as child of %s", prevNode, n.Path)
			n.Children = append(n.Children, prevNode)
			break
		}

		log.Debugf("memory-idx: creating branch %s with child %s", branch, prevNode)
		tree.Items[branch] = &Node{
			Path:     branch,
			Children: []string{prevNode},
			Defs:     make([]schema.MKey, 0),
		}

		prevPos = pos
		pos = strings.LastIndex(branch, ".")
	}

	if pos == -1 {
		// need to add to the root node.
		branch := path[:prevPos]
		log.Debugf("memory-idx: no existing branches found for %s.  Adding to the root node.", branch)
		n := tree.Items[""]
		n.Children = append(n.Children, branch)
	}

	// Add leaf node
	log.Debugf("memory-idx: creating leaf %s", path)
	tree.Items[path] = &Node{
		Path:     path,
		Children: []string{},
		Defs:     []schema.MKey{def.Id},
	}
	m.defById[def.Id] = archive
	statAdd.Inc()

	return
}

func (m *UnpartitionedMemoryIdx) Get(id schema.MKey) (idx.Archive, bool) {
	pre := time.Now()
	m.RLock()
	defer m.RUnlock()
	def, ok := m.defById[id]
	statGetDuration.Value(time.Since(pre))
	if ok {
		return CloneArchive(def), ok
	}
	return idx.Archive{}, ok
}

// GetPath returns the node under the given org and path.
// this is an alternative to Find for when you have a path, not a pattern, and want to lookup in a specific org tree only.
func (m *UnpartitionedMemoryIdx) GetPath(orgId uint32, path string) []idx.Archive {
	m.RLock()
	defer m.RUnlock()
	tree, ok := m.tree[orgId]
	if !ok {
		return nil
	}
	node := tree.Items[path]
	if node == nil {
		return nil
	}
	archives := make([]idx.Archive, len(node.Defs))
	for i, def := range node.Defs {
		archive := m.defById[def]
		archives[i] = CloneArchive(archive)
	}
	return archives
}

func (m *UnpartitionedMemoryIdx) FindByTag(orgId uint32, query tagquery.Query) []idx.Node {
	if !TagSupport {
		log.Warn("memory-idx: received tag query, but tag support is disabled")
		return nil
	}

	queryCtx := NewTagQueryContext(query)

	m.RLock()
	defer m.RUnlock()

	tagIndex, ok := m.tags[orgId]
	if !ok {
		// if there is no tag index, we don't need to run the tag query
		return nil
	}

	var enricher *enricher
	if MetaTagSupport {
		mtr, ok := m.metaTagRecords[orgId]
		if ok {
			enricher = mtr.getEnricher(tagIndex.idHasTag)
		}
	}

	// construct the output slice of idx.Node's such that there is only 1 idx.Node for each path
	resCh := m.idsByTagQuery(orgId, queryCtx)

	byPath := make(map[string]*idx.Node)
	for id := range resCh {
		def, ok := m.defById[id]
		if !ok {
			corruptIndex.Inc()
			log.Errorf("memory-idx: corrupt. ID %q has been given, but it is not in the byId lookup table", id)
			continue
		}

		nameWithTags := def.NameWithTags()
		if existing, ok := byPath[nameWithTags]; !ok {
			byPath[nameWithTags] = &idx.Node{
				Path:        nameWithTags,
				Leaf:        true,
				HasChildren: false,
				Defs:        []idx.Archive{CloneArchive(def)},
			}
			if enricher != nil {
				byPath[nameWithTags].MetaTags = enricher.enrich(def.Id, def.Name, def.Tags)
			}
		} else {
			existing.Defs = append(existing.Defs, CloneArchive(def))
		}
	}

	results := make([]idx.Node, len(byPath))

	i := 0
	for _, v := range byPath {
		results[i] = *v
		i++
	}

	return results
}

// Tags returns a list of all tag keys associated with the metrics of a given
// organization. The return values are filtered by the regex in the second parameter.
func (m *UnpartitionedMemoryIdx) Tags(orgId uint32, filter *regexp.Regexp) []string {
	if !TagSupport {
		log.Warn("memory-idx: received tag query, but tag support is disabled")
		return nil
	}

	m.RLock()
	defer m.RUnlock()

	tags, ok := m.tags[orgId]
	if !ok {
		return nil
	}

	var res []string

	res = make([]string, 0, len(tags))

	for tag := range tags {
		// filter by pattern if one was given
		if filter != nil && !filter.MatchString(tag) {
			continue
		}

		res = append(res, tag)
	}

	if !MetaTagSupport {
		sort.Strings(res)
		return res
	}

	mti, ok := m.metaTagIndex[orgId]
	if !ok {
		sort.Strings(res)
		return res
	}

	for tag := range mti {
		// filter by pattern if one was given
		if filter != nil && !filter.MatchString(tag) {
			continue
		}

		res = append(res, tag)
	}

	sort.Strings(res)
	return res
}

func (m *UnpartitionedMemoryIdx) TagDetails(orgId uint32, key string, filter *regexp.Regexp) map[string]uint64 {
	if !TagSupport {
		log.Warn("memory-idx: received tag query, but tag support is disabled")
		return nil
	}

	m.RLock()
	defer m.RUnlock()

	tags, ok := m.tags[orgId]
	if !ok {
		return nil
	}

	res := make(map[string]uint64)
	for value, ids := range tags[key] {
		if filter != nil && !filter.MatchString(value) {
			continue
		}

		res[value] += uint64(len(ids))
	}

	if !MetaTagSupport {
		return res
	}

	mti, ok := m.metaTagIndex[orgId]
	if !ok {
		return res
	}

	mtr, ok := m.metaTagRecords[orgId]
	if !ok {
		return res
	}

	for value, recordIds := range mti[key] {
		if filter != nil && !filter.MatchString(value) {
			continue
		}

		for _, recordId := range recordIds {
			record, ok := mtr.records[recordId]
			if !ok {
				corruptIndex.Inc()
				log.Errorf("memory-idx: corrupt. record id %d is in meta tag index, but not in meta tag records", recordId)
				continue
			}
			query, err := tagquery.NewQuery(record.Expressions, 0)
			if err != nil {
				corruptIndex.Inc()
				log.Errorf("memory-idx: corrupt. record expressions cannot instantiate query: %+v results in %s", record.Expressions, err)
				continue
			}
			resCh := m.idsByTagQuery(orgId, NewTagQueryContext(query))
			for range resCh {
				res[value]++
			}
		}
	}

	return res
}

// FindTags returns tags matching the specified conditions
// prefix:      prefix match
// limit:       the maximum number of results to return
//
// the results will always be sorted alphabetically for consistency
func (m *UnpartitionedMemoryIdx) FindTags(orgId uint32, prefix string, limit uint) []string {
	if !TagSupport {
		log.Warn("memory-idx: received tag query, but tag support is disabled")
		return nil
	}

	m.RLock()
	defer m.RUnlock()

	tags, ok := m.tags[orgId]
	if !ok {
		return nil
	}

	// probably allocating more than necessary, still better than growing
	res := make([]string, 0, len(tags))

	for tag := range tags {
		// a tag gets appended to the result set if either the given prefix is
		// empty or the tag has the given prefix
		if len(prefix) == 0 || strings.HasPrefix(tag, prefix) {
			res = append(res, tag)
		}
	}

	if !MetaTagSupport {
		return m.finalizeResult(res, limit, false)
	}

	mti, ok := m.metaTagIndex[orgId]
	if !ok {
		return m.finalizeResult(res, limit, false)
	}

	for tag := range mti {
		// a tag gets appended to the result set if either the given prefix is
		// empty or the tag has the given prefix
		if len(prefix) == 0 || strings.HasPrefix(tag, prefix) {
			res = append(res, tag)
		}
	}

	return m.finalizeResult(res, limit, true)
}

// FindTagsWithQuery returns tags matching the specified conditions
// query:       tagdb query to run on the index
// limit:       the maximum number of results to return
//
// the results will always be sorted alphabetically for consistency
func (m *UnpartitionedMemoryIdx) FindTagsWithQuery(orgId uint32, prefix string, query tagquery.Query, limit uint) []string {
	if !TagSupport {
		log.Warn("memory-idx: received tag query, but tag support is disabled")
		return nil
	}

	queryCtx := NewTagQueryContext(query)

	m.RLock()
	defer m.RUnlock()

	tags, ok := m.tags[orgId]
	if !ok {
		return nil
	}

	resMap := make(map[string]struct{})

	var enricher *enricher
	if MetaTagSupport {
		mtr, ok := m.metaTagRecords[orgId]
		if ok {
			enricher = mtr.getEnricher(tags.idHasTag)
		}
	}

	resCh := m.idsByTagQuery(orgId, queryCtx)
	for id := range resCh {
		def, ok := m.defById[id]
		if !ok {
			corruptIndex.Inc()
			log.Errorf("memory-idx: corrupt. ID %q has been given, but it is not in the byId lookup table", id)
			continue
		}

		for _, tag := range def.Tags {
			equal := strings.Index(tag, "=")
			if equal < 0 {
				corruptIndex.Inc()
				log.Errorf("memory-idx: ID %q has tag %q in index without '=' sign", id, tag)
				continue
			}

			key := tag[:equal]
			if len(prefix) == 0 || strings.HasPrefix(key, prefix) {
				resMap[key] = struct{}{}
			}
		}

		if enricher != nil {
			metaTags := enricher.enrich(def.Id, def.Name, def.Tags)
			for _, tag := range metaTags {
				if len(prefix) == 0 || strings.HasPrefix(tag.Key, prefix) {
					resMap[tag.Key] = struct{}{}
				}
			}
		}
	}

	// handle special case of the name tag
	if len(prefix) == 0 || strings.HasPrefix("name", prefix) {
		resMap["name"] = struct{}{}
	}

	res := make([]string, len(resMap))
	i := 0
	for k := range resMap {
		res[i] = k
		i++
	}

	return m.finalizeResult(res, limit, false)
}

// FindTagValues returns tag values matching the specified conditions
// tag:         tag key match
// prefix:      value prefix match
// limit:       the maximum number of results to return
//
// the results will always be sorted alphabetically for consistency
func (m *UnpartitionedMemoryIdx) FindTagValues(orgId uint32, tag, prefix string, limit uint) []string {
	if !TagSupport {
		log.Warn("memory-idx: received tag query, but tag support is disabled")
		return nil
	}

	m.RLock()
	defer m.RUnlock()

	values := m.tags[orgId][tag]

	res := make([]string, 0, len(values))
	for value := range values {
		if len(prefix) == 0 || strings.HasPrefix(value, prefix) {
			res = append(res, value)
		}
	}

	if !MetaTagSupport {
		return m.finalizeResult(res, limit, false)
	}

	metaTagValues := m.metaTagIndex[orgId][tag]
	if len(metaTagValues) == 0 {
		return m.finalizeResult(res, limit, false)
	}

	for value := range metaTagValues {
		// a tag value gets appended to the result set if either the given prefix is
		// empty or the tag value has the given prefix
		if len(prefix) == 0 || strings.HasPrefix(value, prefix) {
			res = append(res, value)
		}
	}

	return m.finalizeResult(res, limit, true)
}

func (m *UnpartitionedMemoryIdx) FindTagValuesWithQuery(orgId uint32, tag, prefix string, query tagquery.Query, limit uint) []string {
	if !TagSupport {
		log.Warn("memory-idx: received tag query, but tag support is disabled")
		return nil
	}

	queryCtx := NewTagQueryContext(query)

	m.RLock()
	defer m.RUnlock()

	tags, ok := m.tags[orgId]
	if !ok {
		return nil
	}

	resMap := make(map[string]struct{})

	var enricher *enricher
	if MetaTagSupport {
		mtr, ok := m.metaTagRecords[orgId]
		if ok {
			enricher = mtr.getEnricher(tags.idHasTag)
		}
	}

	resCh := m.idsByTagQuery(orgId, queryCtx)
	tagPrefix := tag + "=" + prefix
	for id := range resCh {
		def, ok := m.defById[id]
		if !ok {
			corruptIndex.Inc()
			log.Errorf("memory-idx: corrupt. ID %q has been given, but it is not in the byId lookup table", id)
			continue
		}

		// special case if the tag to complete values for is "name"
		if tag == "name" {
			if strings.HasPrefix(def.NameSanitizedAsTagValue(), prefix) {
				resMap[def.NameSanitizedAsTagValue()] = struct{}{}
			}
		} else {
			for _, tag := range def.Tags {
				if !strings.HasPrefix(tag, tagPrefix) {
					continue
				}

				tagValue := strings.SplitN(tag, "=", 2)
				if len(tagValue) < 2 {
					// should never happen because invalid tags should get rejected at ingestion
					corruptIndex.Inc()
					log.Errorf("memory-idx: tag \"%s\" is invalid because it has no \"=\"", tag)
					continue
				}

				resMap[tagValue[1]] = struct{}{}
			}

			if enricher != nil {
				metaTags := enricher.enrich(def.Id, def.Name, def.Tags)
				for _, metaTag := range metaTags {
					if metaTag.Key == tag && strings.HasPrefix(metaTag.Value, prefix) {
						resMap[metaTag.Value] = struct{}{}
					}
				}
			}
		}
	}

	res := make([]string, len(resMap))
	i := 0
	for v := range resMap {
		res[i] = v
		i++
	}

	return m.finalizeResult(res, limit, false)
}

func (m *UnpartitionedMemoryIdx) idsByTagQuery(orgId uint32, query TagQueryContext) chan schema.MKey {
	resCh := make(chan schema.MKey, 100)

	tags, ok := m.tags[orgId]
	if !ok {
		close(resCh)
		return resCh
	}

	query.RunNonBlocking(tags, m.defById, m.metaTagIndex[orgId], m.metaTagRecords[orgId], resCh)

	return resCh
}

// finalizeResult prepares a result to return it to the caller
// it takes a slice of results which needs to be finalized and prepared to be returned
// a limit that defines how many results we want to return, if the final results set is more than limit
// then it limits the result to the given size. the result set gets sorted before the limitation is
// applied, to ensure consistent result sets
// a deduplicate flag, if the deduplicate flag is true then it deduplicates the results before returning
func (m *UnpartitionedMemoryIdx) finalizeResult(res []string, limit uint, deduplicate bool) []string {
	sort.Strings(res)

	if deduplicate && len(res) >= 2 {
		j := 0
		for i := 1; i < len(res); i++ {
			if res[j] == res[i] {
				continue
			}
			j++
			res[j] = res[i]
		}
		res = res[:j+1]
	}

	if len(res) > int(limit) {
		return res[:limit]
	}

	return res
}

func (m *UnpartitionedMemoryIdx) findMaybeCached(tree *Tree, orgId uint32, pattern string) ([]*Node, error) {

	if m.findCache == nil {
		return find(tree, pattern)
	}

	matchedNodes, ok := m.findCache.Get(orgId, pattern)
	if ok {
		return matchedNodes, nil
	}

	matchedNodes, err := find(tree, pattern)
	if err != nil {
		return nil, err
	}
	m.findCache.Add(orgId, pattern, matchedNodes)
	return matchedNodes, nil
}

func (m *UnpartitionedMemoryIdx) Find(orgId uint32, pattern string, from int64) ([]idx.Node, error) {
	pre := time.Now()
	var matchedNodes []*Node
	var err error
	m.RLock()
	defer m.RUnlock()
	tree, ok := m.tree[orgId]
	if !ok {
		log.Debugf("memory-idx: orgId %d has no metrics indexed.", orgId)
	} else {
		matchedNodes, err = m.findMaybeCached(tree, orgId, pattern)
		if err != nil {
			return nil, err
		}
	}
	if orgId != idx.OrgIdPublic && idx.OrgIdPublic > 0 {
		tree, ok = m.tree[idx.OrgIdPublic]
		if ok {
			publicNodes, err := m.findMaybeCached(tree, idx.OrgIdPublic, pattern)
			if err != nil {
				return nil, err
			}
			matchedNodes = append(matchedNodes, publicNodes...)
		}
	}
	log.Debugf("memory-idx: %d nodes matching pattern %s found", len(matchedNodes), pattern)
	results := make([]idx.Node, 0)
	byPath := make(map[string]struct{})
	// construct the output slice of idx.Node's such that there is only 1 idx.Node
	// for each path, and it holds all defs that the Node refers too.
	// if there are public (orgId OrgIdPublic) and private leaf nodes with the same series
	// path, then the public metricDefs will be excluded.
	for _, n := range matchedNodes {
		if _, ok := byPath[n.Path]; !ok {
			idxNode := idx.Node{
				Path:        n.Path,
				Leaf:        n.Leaf(),
				HasChildren: n.HasChildren(),
			}
			if idxNode.Leaf {
				idxNode.Defs = make([]idx.Archive, 0, len(n.Defs))
				for _, id := range n.Defs {
					def := m.defById[id]
					if def == nil {
						// def could be nil if items from the findCache have been deleted.
						log.Debugf("memory-idx: Find: def with id=%s from node with path=%s does not exist anymore", id, n.Path)
						continue
					}
					if from != 0 && atomic.LoadInt64(&def.LastUpdate) < from {
						statFiltered.Inc()
						log.Debugf("memory-idx: from is %d, so skipping %s which has LastUpdate %d", from, def.Id, atomic.LoadInt64(&def.LastUpdate))
						continue
					}
					if log.IsLevelEnabled(log.DebugLevel) {
						lastSave := atomic.LoadUint32(&def.LastSave)
						log.Debugf("memory-idx: Find: adding to path %s archive id=%s name=%s int=%d schemaId=%d aggId=%d irId=%d lastSave=%d", n.Path, def.Id, def.Name, def.Interval, def.SchemaId, def.AggId, def.IrId, lastSave)
					}
					idxNode.Defs = append(idxNode.Defs, CloneArchive(def))
				}
				if len(idxNode.Defs) == 0 {
					continue
				}
			}
			results = append(results, idxNode)
			byPath[n.Path] = struct{}{}
		} else {
			log.Debugf("memory-idx: path %s already seen", n.Path)
		}
	}
	log.Debugf("memory-idx: %d nodes has %d unique paths.", len(matchedNodes), len(results))
	statFindDuration.Value(time.Since(pre))
	return results, nil
}

// find returns all Nodes matching the pattern for the given tree
func find(tree *Tree, pattern string) ([]*Node, error) {
	var nodes []string
	if strings.Index(pattern, ";") == -1 {
		nodes = strings.Split(pattern, ".")
	} else {
		nodes = strings.SplitN(pattern, ";", 2)
		tags := nodes[1]
		nodes = strings.Split(nodes[0], ".")
		nodes[len(nodes)-1] += ";" + tags
	}

	// pos is the index of the first node with special chars, or one past the last node if exact
	// for a query like foo.bar.baz, pos is 3
	// for a query like foo.bar.* or foo.bar, pos is 2
	// for a query like foo.b*.baz, pos is 1
	pos := len(nodes)
	for i := 0; i < len(nodes); i++ {
		if strings.ContainsAny(nodes[i], "*{}[]?") {
			log.Debugf("memory-idx: found first pattern sequence at node %s pos %d", nodes[i], i)
			pos = i
			break
		}
	}
	var branch string
	if pos != 0 {
		branch = strings.Join(nodes[:pos], ".")
	}
	log.Debugf("memory-idx: starting search at node %q", branch)
	startNode, ok := tree.Items[branch]

	if !ok {
		log.Debugf("memory-idx: branch %q does not exist in the index", branch)
		return nil, nil
	}

	if startNode == nil {
		corruptIndex.Inc()
		log.Errorf("memory-idx: startNode is nil. patt=%q,pos=%d,branch=%q", pattern, pos, branch)
		return nil, errors.NewInternal("hit an empty path in the index")
	}

	children := []*Node{startNode}
	for i := pos; i < len(nodes); i++ {
		p := nodes[i]

		matcher, err := getMatcher(p)

		if err != nil {
			return nil, err
		}

		var grandChildren []*Node
		for _, c := range children {
			if !c.HasChildren() {
				log.Debugf("memory-idx: end of branch reached at %s with no match found for %s", c.Path, pattern)
				// expecting a branch
				continue
			}
			log.Debugf("memory-idx: searching %d children of %s that match %s", len(c.Children), c.Path, nodes[i])
			matches := matcher(c.Children)
			for _, m := range matches {
				newBranch := c.Path + "." + m
				if c.Path == "" {
					newBranch = m
				}
				grandChild := tree.Items[newBranch]
				if grandChild == nil {
					corruptIndex.Inc()
					log.Errorf("memory-idx: grandChild is nil. patt=%q,i=%d,pos=%d,p=%q,path=%q", pattern, i, pos, p, newBranch)
					return nil, errors.NewInternal("hit an empty path in the index")
				}

				grandChildren = append(grandChildren, grandChild)
			}
		}
		children = grandChildren
		if len(children) == 0 {
			log.Debug("memory-idx: pattern does not match any series.")
			break
		}
	}

	log.Debugf("memory-idx: reached pattern length. %d nodes matched", len(children))
	return children, nil
}

func (m *UnpartitionedMemoryIdx) List(orgId uint32) []idx.Archive {
	pre := time.Now()
	m.RLock()
	defer m.RUnlock()

	defs := make([]idx.Archive, 0)
	for _, def := range m.defById {
		if def.OrgId == orgId || def.OrgId == idx.OrgIdPublic {
			defs = append(defs, CloneArchive(def))
		}
	}

	statListDuration.Value(time.Since(pre))

	return defs
}

func (m *UnpartitionedMemoryIdx) DeleteTagged(orgId uint32, query tagquery.Query) []idx.Archive {
	if !TagSupport {
		log.Warn("memory-idx: received tag query, but tag support is disabled")
		return nil
	}

	queryCtx := NewTagQueryContext(query)

	m.RLock()
	resCh := m.idsByTagQuery(orgId, queryCtx)
	ids := make(IdSet)
	for id := range resCh {
		ids[id] = struct{}{}
	}

	m.RUnlock()

	m.Lock()
	defer m.Unlock()
	return m.deleteTaggedByIdSet(orgId, ids)
}

// deleteTaggedByIdSet deletes a map of ids from the tag index and also the DefByIds
// it is important that only IDs of series with tags get passed in here, because
// otherwise the result might be inconsistencies between DefByIDs and the tree index.
func (m *UnpartitionedMemoryIdx) deleteTaggedByIdSet(orgId uint32, ids IdSet) []idx.Archive {
	tags, ok := m.tags[orgId]
	if !ok {
		return nil
	}

	deletedDefs := make([]idx.Archive, 0, len(ids))
	for id := range ids {
		idStr := id
		def, ok := m.defById[idStr]
		if !ok {
			// not necessarily a corruption, the id could have been deleted
			// while we switched from read to write lock
			continue
		}
		if !m.deindexTags(tags, &def.MetricDefinition) {
			continue
		}
		deletedDefs = append(deletedDefs, CloneArchive(def))
		delete(m.defById, idStr)
	}

	statMetricsActive.DecUint32(uint32(len(deletedDefs)))

	return deletedDefs
}

func (m *UnpartitionedMemoryIdx) Delete(orgId uint32, pattern string) ([]idx.Archive, error) {
	var deletedDefs []idx.Archive
	pre := time.Now()
	m.Lock()
	defer func() {
		m.Unlock()
		if len(deletedDefs) == 0 {
			return
		}
		if m.findCache == nil {
			return
		}
		// asynchronously invalidate any findCache entries
		// that match any of the deleted series.
		if len(deletedDefs) > findCacheInvalidateQueueSize {
			m.findCache.Purge(orgId)
		} else {
			for _, d := range deletedDefs {
				m.findCache.InvalidateFor(orgId, d.NameWithTags())
			}
		}
	}()
	tree, ok := m.tree[orgId]
	if !ok {
		return nil, nil
	}
	found, err := find(tree, pattern)
	if err != nil {
		return nil, err
	}

	for _, f := range found {
		deleted := m.delete(orgId, f, true, true)
		deletedDefs = append(deletedDefs, deleted...)
	}

	statMetricsActive.DecUint32(uint32(len(deletedDefs)))
	statDeleteDuration.Value(time.Since(pre))

	return deletedDefs, nil
}

func (m *UnpartitionedMemoryIdx) delete(orgId uint32, n *Node, deleteEmptyParents, deleteChildren bool) []idx.Archive {
	tree := m.tree[orgId]
	deletedDefs := make([]idx.Archive, 0)
	if deleteChildren && n.HasChildren() {
		log.Debugf("memory-idx: deleting branch %s", n.Path)
		// walk up the tree to find all leaf nodes and delete them.
		for _, child := range n.Children {
			node, ok := tree.Items[n.Path+"."+child]
			if !ok {
				corruptIndex.Inc()
				log.Errorf("memory-idx: node %q missing. Index is corrupt.", n.Path+"."+child)
				continue
			}
			log.Debugf("memory-idx: deleting child %s from branch %s", node.Path, n.Path)
			deleted := m.delete(orgId, node, false, true)
			deletedDefs = append(deletedDefs, deleted...)
		}
		n.Children = nil
	}

	// delete the metricDefs
	for _, id := range n.Defs {
		log.Debugf("memory-idx: deleting %s from index", id)
		archivePointer, ok := m.defById[id]
		if archivePointer == nil {
			corruptIndex.Inc()
			log.Errorf("memory-idx: UnpartitionedMemoryIdx.delete() Index is corrupt. nil, %t := defById[%s]. path=%s ", ok, id.String(), n.Path)
			continue
		}
		deletedDefs = append(deletedDefs, CloneArchive(archivePointer))
		delete(m.defById, id)
	}

	n.Defs = nil

	if n.HasChildren() {
		return deletedDefs
	}

	// delete the node.
	delete(tree.Items, n.Path)

	if !deleteEmptyParents {
		return deletedDefs
	}

	// delete node from the branches
	// e.g. for foo.bar.baz
	// branch "foo.bar" -> node "baz"
	// branch "foo"     -> node "bar"
	// branch ""        -> node "foo"
	nodes := strings.Split(n.Path, ".")
	for i := len(nodes) - 1; i >= 0; i-- {
		branch := strings.Join(nodes[:i], ".")
		log.Debugf("memory-idx: removing %s from branch %s", nodes[i], branch)
		bNode, ok := tree.Items[branch]
		if !ok {
			corruptIndex.Inc()
			log.Errorf("memory-idx: node %s missing. Index is corrupt.", branch)
			continue
		}
		if len(bNode.Children) > 1 {
			newChildren := make([]string, 0, len(bNode.Children)-1)
			for _, child := range bNode.Children {
				if child != nodes[i] {
					newChildren = append(newChildren, child)
				} else {
					log.Debugf("memory-idx: %s removed from children list of branch %s", child, bNode.Path)
				}
			}
			bNode.Children = newChildren
			log.Debugf("memory-idx: branch %s has other children. Leaving it in place", bNode.Path)
			// no need to delete any parents as they are needed by this node and its
			// remaining children
			break
		}

		if len(bNode.Children) == 0 {
			corruptIndex.Inc()
			log.Errorf("memory-idx: branch %s has no children while trying to delete %s. Index is corrupt", branch, nodes[i])
			break
		}

		if bNode.Children[0] != nodes[i] {
			corruptIndex.Inc()
			log.Errorf("memory-idx: %s not in children list for branch %s. Index is corrupt", nodes[i], branch)
			break
		}
		bNode.Children = nil
		if bNode.Leaf() {
			log.Debugf("memory-idx: branch %s is also a leaf node, keeping it.", branch)
			break
		}
		log.Debugf("memory-idx: branch %s has no children and is not a leaf node, deleting it.", branch)
		delete(tree.Items, branch)
	}

	return deletedDefs
}

// Prune prunes series from the index if they have become stale per their index-rule
func (m *UnpartitionedMemoryIdx) Prune(now time.Time) ([]idx.Archive, error) {
	log.Info("memory-idx: start pruning of series across all orgs")
	orgs := make(map[uint32]struct{})
	m.RLock()
	for org := range m.tree {
		orgs[org] = struct{}{}
	}
	if TagSupport {
		for org := range m.tags {
			orgs[org] = struct{}{}
		}
	}
	m.RUnlock()

	var pruned []idx.Archive
	toPruneUntagged := make(map[uint32]map[string]struct{}, len(orgs))
	toPruneTagged := make(map[uint32]IdSet, len(orgs))
	for org := range orgs {
		toPruneTagged[org] = make(IdSet)
		toPruneUntagged[org] = make(map[string]struct{})
	}
	pre := time.Now()

	// getting all cutoffs once saves having to recompute everytime we have a match
	cutoffs := IndexRules.Cutoffs(now)

	m.RLock()

DEFS:
	for _, def := range m.defById {
		cutoff := cutoffs[def.IrId]
		if atomic.LoadInt64(&def.LastUpdate) >= cutoff {
			continue DEFS
		}

		if len(def.Tags) == 0 {
			tree, ok := m.tree[def.OrgId]
			if !ok {
				continue DEFS
			}

			n, ok := tree.Items[def.Name]
			if !ok || !n.Leaf() {
				continue DEFS
			}

			for _, id := range n.Defs {
				if atomic.LoadInt64(&m.defById[id].LastUpdate) >= cutoff {
					continue DEFS
				}
			}

			toPruneUntagged[def.OrgId][n.Path] = struct{}{}
		} else {
			defName := def.NameWithTags()
			defs := m.defByTagSet.defs(def.OrgId, defName)
			// if any other MetricDef with the same tag set is not expired yet,
			// then we do not want to prune any of them
			for sdef := range defs {
				if atomic.LoadInt64(&sdef.LastUpdate) >= cutoff {
					continue DEFS
				}
			}

			for sdef := range defs {
				if defName != sdef.NameWithTags() {
					corruptIndex.Inc()
					log.Errorf("Almost added bad def to prune list: def=%v, sdef=%v", def, sdef)
				} else {
					toPruneTagged[sdef.OrgId][sdef.Id] = struct{}{}
				}
			}
		}
	}
	m.RUnlock()

	// create a new timeLimiter that allows us to limit the amount of time we spend
	// holding a lock to maxPruneLockTime (default 100ms) every second.
	tl := NewTimeLimiter(time.Second, maxPruneLockTime, time.Now())

	for org, ids := range toPruneTagged {
		if len(ids) == 0 {
			continue
		}
		// make sure we are not locking for too long.
		tl.Wait()
		lockStart := time.Now()
		m.Lock()
		defs := m.deleteTaggedByIdSet(org, ids)
		m.Unlock()
		tl.Add(time.Since(lockStart))
		pruned = append(pruned, defs...)
	}

	// need to capture how many were already deleted and decremented by using deleteTaggedByIdSet
	// so we can subtract it later on before we decrement the active metrics stat
	totalDeletedByTag := len(pruned)

ORGS:
	for org, paths := range toPruneUntagged {
		if len(paths) == 0 {
			continue
		}

		for path := range paths {
			tl.Wait()
			lockStart := time.Now()
			m.Lock()
			tree, ok := m.tree[org]

			if !ok {
				m.Unlock()
				tl.Add(time.Since(lockStart))
				continue ORGS
			}

			n, ok := tree.Items[path]

			if !ok {
				m.Unlock()
				tl.Add(time.Since(lockStart))
				log.Debugf("memory-idx: series %s for orgId:%d was identified for pruning but cannot be found.", path, org)
				continue
			}

			log.Debugf("memory-idx: series %s for orgId:%d is stale. pruning it.", n.Path, org)
			defs := m.delete(org, n, true, false)
			m.Unlock()
			tl.Add(time.Since(lockStart))
			pruned = append(pruned, defs...)
		}
		if m.findCache != nil {
			if len(paths) > findCacheInvalidateQueueSize {
				m.findCache.Purge(org)
			} else {
				for path := range paths {
					m.findCache.InvalidateFor(org, path)
				}
			}
		}
	}

	statMetricsActive.DecUint32(uint32(len(pruned) - totalDeletedByTag))

	duration := time.Since(pre)
	log.Infof("memory-idx: finished pruning of %d series in %s", len(pruned), duration)

	statPruneDuration.Value(duration)
	return pruned, nil
}

func getMatcher(path string) (func([]string) []string, error) {
	// Matches everything
	if path == "*" {
		return func(children []string) []string {
			log.Debug("memory-idx: Matching all children")
			return children
		}, nil
	}

	var patterns []string
	if strings.ContainsAny(path, "{}") {
		patterns = expandQueries(path)
	} else {
		patterns = []string{path}
	}

	// Convert to regex and match
	if strings.ContainsAny(path, "*[]?") {
		regexes := make([]*regexp.Regexp, 0, len(patterns))
		for _, p := range patterns {
			r, err := regexp.Compile(toRegexp(p))
			if err != nil {
				log.Debugf("memory-idx: regexp failed to compile. %s - %s", p, err)
				return nil, errors.NewBadRequest(err.Error())
			}
			regexes = append(regexes, r)
		}

		return func(children []string) []string {
			var matches []string
			for _, r := range regexes {
				for _, c := range children {
					if r.MatchString(c) {
						log.Debugf("memory-idx: %s =~ %s", c, r.String())
						matches = append(matches, c)
					}
				}
			}
			return matches
		}, nil
	}

	// Exact match one or more values
	return func(children []string) []string {
		var results []string
		for _, p := range patterns {
			for _, c := range children {
				if c == p {
					log.Debugf("memory-idx: %s matches %s", c, p)
					results = append(results, c)
					break
				}
			}
		}
		return results
	}, nil
}

// We don't use filepath.Match as it doesn't support {} because that's not posix, it's a bashism
// the easiest way of implementing this extra feature is just expanding single queries
// that contain these queries into multiple queries, which will be checked separately
// and the results of which will be ORed.
func expandQueries(query string) []string {
	queries := []string{query}

	// as long as we find a { followed by a }, split it up into subqueries, and process
	// all queries again
	// we only stop once there are no more queries that still have {..} in them
	keepLooking := true
	for keepLooking {
		expanded := make([]string, 0)
		keepLooking = false
		for _, query := range queries {
			lbrace := strings.Index(query, "{")
			rbrace := -1
			if lbrace > -1 {
				rbrace = strings.Index(query[lbrace:], "}")
				if rbrace > -1 {
					rbrace += lbrace
				}
			}

			if lbrace > -1 && rbrace > -1 {
				keepLooking = true
				expansion := query[lbrace+1 : rbrace]
				options := strings.Split(expansion, ",")
				for _, option := range options {
					expanded = append(expanded, query[:lbrace]+option+query[rbrace+1:])
				}
			} else {
				expanded = append(expanded, query)
			}
		}
		queries = expanded
	}
	return queries
}

func toRegexp(pattern string) string {
	p := pattern
	p = strings.Replace(p, "*", ".*", -1)
	p = strings.Replace(p, "?", ".?", -1)
	p = "^" + p + "$"
	return p
}

// CloneArchive safely clones an archive. We use atomic operations to update
// fields, so we need to use atomic operations to read those fields
// when copying.
func CloneArchive(a *idx.Archive) idx.Archive {
	return idx.Archive{
		SchemaId:         a.SchemaId,
		AggId:            a.AggId,
		IrId:             a.IrId,
		LastSave:         atomic.LoadUint32(&a.LastSave),
		MetricDefinition: a.MetricDefinition.Clone(),
	}
}
