package memory

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
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
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/schema"
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

	// metric idx.memory.intern.memory is the total memory used by the interning layer in bytes by object size
	statInternMemory = make([]*stats.Gauge64, 256)
	// metric idx.memory.intern.fragmentation is the total fragmentation percent of the object store used by the interning layer by object size
	statInternFragmentation = make([]*stats.Gauge32, 256)

	Enabled                      bool
	matchCacheSize               int
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
)

func init() {
	// instantiate stats for the interning layer/object store
	for i := 0; i < 256; i++ {
		statInternMemory[i] = stats.NewGauge64(fmt.Sprintf("idx.memory.intern.memory.%d", i))
		statInternFragmentation[i] = stats.NewGauge32(fmt.Sprintf("idx.memory.intern.fragmentation.%d", i))
	}
}

func ConfigSetup() {
	memoryIdx := flag.NewFlagSet("memory-idx", flag.ExitOnError)
	memoryIdx.BoolVar(&Enabled, "enabled", false, "")
	memoryIdx.BoolVar(&TagSupport, "tag-support", false, "enables/disables querying based on tags")
	memoryIdx.BoolVar(&Partitioned, "partitioned", false, "use separate indexes per partition. experimental feature")
	memoryIdx.IntVar(&TagQueryWorkers, "tag-query-workers", 50, "number of workers to spin up to evaluate tag queries")
	memoryIdx.IntVar(&matchCacheSize, "match-cache-size", 1000, "size of regular expression cache in tag query evaluation")
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
	globalconf.Register("memory-idx", memoryIdx, flag.ExitOnError)
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

}

// MemoryIndex is an interface implemented by both UnpartitionedMemoryIdx and PartitionedMemoryIdx
// this is needed to support unit tests.
type MemoryIndex interface {
	idx.MetricIndex
	LoadPartition(int32, []idx.MetricDefinition) int
	UpdateArchiveLastSave(schema.MKey, int32, uint32)
	add(*idx.Archive)
	idsByTagQuery(uint32, TagQueryContext) IdSet
	PurgeFindCache()
	ForceInvalidationFindCache()
}

func New() MemoryIndex {
	if Partitioned {
		return NewPartitionedMemoryIdx()
	}
	return NewUnpartitionedMemoryIdx()
}

// Tree is the top level struct in the hierarchical index for a given OrgID
type Tree struct {
	Items map[string]*Node // key is the full path of the node.
}

type IdSet map[schema.MKey]struct{}

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

type TagValues map[uintptr]IdSet    // value -> set of ids
type TagIndex map[uintptr]TagValues // key -> list of values

func (t *TagIndex) addTagId(name, value uintptr, id schema.MKey) {
	ti := *t
	if _, ok := ti[name]; !ok {
		ti[name] = make(TagValues)
	}
	if _, ok := ti[name][value]; !ok {
		ti[name][value] = make(IdSet)
	}
	ti[name][value][id] = struct{}{}
}

func (t *TagIndex) delTagId(name, value uintptr, id schema.MKey) {
	ti := *t

	delete(ti[name][value], id)

	if len(ti[name][value]) == 0 {
		delete(ti[name], value)
		if len(ti[name]) == 0 {
			delete(ti, name)
		}
	}
}

// org id -> NameWithTagsHash() -> Map keyed by *idx.MetricDefinition.
type defByTagSet map[uint32]map[idx.Md5Hash]map[*idx.MetricDefinition]struct{}

func (defs defByTagSet) add(def *idx.MetricDefinition) {
	var orgDefs map[idx.Md5Hash]map[*idx.MetricDefinition]struct{}
	var ok bool
	if orgDefs, ok = defs[def.OrgId]; !ok {
		orgDefs = make(map[idx.Md5Hash]map[*idx.MetricDefinition]struct{})
		defs[def.OrgId] = orgDefs
	}

	hashedName := def.NameWithTagsHash()
	if _, ok = orgDefs[hashedName]; !ok {
		orgDefs[hashedName] = make(map[*idx.MetricDefinition]struct{}, 1)
	}

	if _, ok = orgDefs[hashedName][def]; !ok {
		orgDefs[hashedName][def] = struct{}{}
	}
}

func (defs defByTagSet) del(def *idx.MetricDefinition) {
	var orgDefs map[idx.Md5Hash]map[*idx.MetricDefinition]struct{}
	var ok bool
	if orgDefs, ok = defs[def.OrgId]; !ok {
		return
	}

	hashedName := def.NameWithTagsHash()
	delete(orgDefs[hashedName], def)

	if len(orgDefs[hashedName]) == 0 {
		delete(orgDefs, hashedName)
	}

	if len(orgDefs) == 0 {
		delete(defs, def.OrgId)
	}
}

func (defs defByTagSet) defs(id uint32, fullName string) map[*idx.MetricDefinition]struct{} {
	var orgDefs map[idx.Md5Hash]map[*idx.MetricDefinition]struct{}
	var ok bool
	if orgDefs, ok = defs[id]; !ok {
		return nil
	}

	md5Sum := md5.Sum(bytes.NewBufferString(fullName).Bytes())
	hashedName := idx.Md5Hash{
		Upper: binary.LittleEndian.Uint64(md5Sum[:8]),
		Lower: binary.LittleEndian.Uint64(md5Sum[8:]),
	}

	ret := make(map[*idx.MetricDefinition]struct{})

	//TODO: Possibly refactor by adding a method to compare
	//		based on uintptrs instead of strings
	for def := range orgDefs[hashedName] {
		if fullName != def.NameWithTags() {
			corruptIndex.Inc()
			log.Errorf("There is a collision in HashedNames: def=%v, fullName=%v\n", def, fullName)
		} else {
			ret[def] = struct{}{}
		}
	}

	return ret
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
	tags           map[uint32]TagIndex       // by orgId
	metaTags       map[uint32]metaTagIndex   // by orgId
	metaTagRecords map[uint32]metaTagRecords // by orgId

	// used to reduce contention
	findCache *FindCache

	writeQueue *WriteQueue

	// used to stop interning layer stats reporting
	shutdown chan struct{}
}

func NewUnpartitionedMemoryIdx() *UnpartitionedMemoryIdx {
	umi := UnpartitionedMemoryIdx{
		defById:        make(map[schema.MKey]*idx.Archive),
		defByTagSet:    make(defByTagSet),
		tree:           make(map[uint32]*Tree),
		tags:           make(map[uint32]TagIndex),
		metaTags:       make(map[uint32]metaTagIndex),
		metaTagRecords: make(map[uint32]metaTagRecords),
	}

	return &umi
}

func (m *UnpartitionedMemoryIdx) Init() error {
	if findCacheSize > 0 {
		m.findCache = NewFindCache(findCacheSize, findCacheInvalidateQueueSize, findCacheInvalidateMaxSize, findCacheInvalidateMaxWait, findCacheBackoffTime)
	}
	if writeQueueEnabled {
		m.writeQueue = NewWriteQueue(m, writeQueueDelay, writeMaxBatchSize)
	}

	m.shutdown = make(chan struct{})

	// gather memory and fragmentation statistics on the object store every minute
	go func(shutdown chan struct{}) {
		for {
			select {
			case <-shutdown:
				return
			default:
				m.Lock()
				for _, internMemStat := range idx.IdxIntern.MemStatsPerPool() {
					statInternMemory[internMemStat.ObjSize].SetUint64(internMemStat.MemUsed)
				}
				for _, internFragStat := range idx.IdxIntern.FragStatsPerPool() {
					statInternFragmentation[internFragStat.ObjSize].SetUint32(uint32(100 - (internFragStat.FragPercent * 100)))
				}
				m.Unlock()
			}
			time.Sleep(time.Minute)
		}
	}(m.shutdown)

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
	close(m.shutdown)

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
func (m *UnpartitionedMemoryIdx) Update(point schema.MetricPoint, partition int32) (*idx.Archive, int32, bool) {
	pre := time.Now()

	m.RLock()
	existing, ok := m.defById[point.MKey]
	m.RUnlock()
	if ok {
		if log.IsLevelEnabled(log.DebugLevel) {
			log.Debugf("memory-idx: metricDef with id %v already in index", point.MKey)
		}

		oldPart := updateExisting(existing, partition, int64(point.Time), pre)
		return idx.NewSafeArchive(existing), oldPart, true
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
			return idx.NewSafeArchive(existing), oldPart, true
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
			return idx.NewSafeArchive(existing), oldPart, true
		}
	}

	return nil, 0, false
}

// AddOrUpdate returns the corresponding Archive for the MetricData.
// if it is existing -> updates lastUpdate based on .Time, and partition
// if was new        -> adds new MetricDefinition to index
func (m *UnpartitionedMemoryIdx) AddOrUpdate(mkey schema.MKey, data *schema.MetricData, partition int32) (*idx.Archive, int32, bool) {
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
		return idx.NewSafeArchive(existing), oldPart, ok
	}

	def, err := idx.MetricDefinitionFromMetricDataWithMKey(mkey, data)
	if err != nil {
		return nil, 0, false
	}
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

	return idx.NewSafeArchive(archive), 0, false
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
// The return values are:
// 1) The relevant meta record as it is after this operation
// 2) A bool that is true if the record has been created, or false if updated
// 3) An error which is nil if no error has occurred
func (m *UnpartitionedMemoryIdx) MetaTagRecordUpsert(orgId uint32, upsertRecord tagquery.MetaTagRecord) (tagquery.MetaTagRecord, bool, error) {
	res := tagquery.MetaTagRecord{}

	if !TagSupport {
		log.Warn("memory-idx: received meta-tag query, but tag support is disabled")
		return res, false, errors.NewBadRequest("Tag support is disabled")
	}

	var mtr metaTagRecords
	var mti metaTagIndex
	var ok bool

	m.Lock()
	defer m.Unlock()

	if mtr, ok = m.metaTagRecords[orgId]; !ok {
		mtr = make(metaTagRecords)
		m.metaTagRecords[orgId] = mtr
	}

	if mti, ok = m.metaTags[orgId]; !ok {
		mti = make(metaTagIndex)
		m.metaTags[orgId] = mti
	}

	id, record, oldId, oldRecord, err := mtr.upsert(upsertRecord)
	if err != nil {
		return res, false, err
	}
	res = *record

	// if this upsert has updated a previously existing record, then we remove its entries
	// from the metaTagIndex before inserting the new ones
	if oldRecord != nil {
		for _, keyValue := range oldRecord.MetaTags {
			mti.deleteRecord(keyValue, oldId)
		}

		for _, keyValue := range record.MetaTags {
			mti.insertRecord(keyValue, id)
		}

		return res, false, nil
	}

	for _, keyValue := range record.MetaTags {
		mti.insertRecord(keyValue, id)
	}

	return res, true, nil
}

func (m *UnpartitionedMemoryIdx) MetaTagRecordList(orgId uint32) []tagquery.MetaTagRecord {
	var res []tagquery.MetaTagRecord

	m.RLock()
	defer m.RUnlock()

	metaTagRecords, ok := m.metaTagRecords[orgId]
	if !ok {
		return res
	}

	res = make([]tagquery.MetaTagRecord, len(metaTagRecords))
	i := 0
	for _, record := range metaTagRecords {
		res[i] = record
		i++
	}

	return res
}

// indexTags reads the tags of a given metric definition and creates the
// corresponding tag index entries to refer to it. It assumes a lock is
// already held.
func (m *UnpartitionedMemoryIdx) indexTags(def *idx.MetricDefinition) {
	tags, ok := m.tags[def.OrgId]
	if !ok {
		tags = make(TagIndex)
		m.tags[def.OrgId] = tags
	}

	for _, tag := range def.Tags.KeyValues {
		tags.addTagId(tag.Key, tag.Value, def.Id)
	}
	// TODO: add special case to handle name and intern each node separately
	nameKey, _ := idx.IdxIntern.AddOrGet([]byte("name"), false)
	nameValue, _ := idx.IdxIntern.AddOrGet([]byte(schema.SanitizeNameAsTagValue(def.Name.String())), false)
	tags.addTagId(nameKey, nameValue, def.Id)

	// Added sort here to accommodate a case where out of order tags are set after
	// a call to SetId (which would sort it)
	// e.g. the case where somebody sent us a MD with an id already set and out-of-order tags
	sort.Sort(def.Tags.KeyValues)

	m.defByTagSet.add(def)
}

// deindexTags takes a given metric definition and removes all references
// to it from the tag index. It assumes a lock is already held.
// a return value of "false" means there was an error and the deindexing was
// unsuccessful, "true" means the indexing was at least partially or completely
// successful
func (m *UnpartitionedMemoryIdx) deindexTags(tags TagIndex, def *idx.MetricDefinition) bool {
	for _, tag := range def.Tags.KeyValues {
		tags.delTagId(tag.Key, tag.Value, def.Id)
	}

	nameKey, err := idx.IdxIntern.GetPtrFromByte([]byte("name"))
	if err != nil {
		log.Error("memory-idx: Failed to retrieve interned string for tag key: ", err)
		internError.Inc()
		return true
	}
	nameValue, err := idx.IdxIntern.GetPtrFromByte([]byte(schema.SanitizeNameAsTagValue(def.Name.String())))
	if err != nil {
		log.Error("memory-idx: Failed to retrieve interned string for tag value: ", err)
		internError.Inc()
		return true
	}
	tags.delTagId(nameKey, nameValue, def.Id)

	m.defByTagSet.del(def)

	return true
}

// LoadPartition is used to rebuild the index from an existing set of metricDefinitions for a specific paritition.
func (m *UnpartitionedMemoryIdx) LoadPartition(partition int32, defs []idx.MetricDefinition) int {
	// UnpartitionedMemoryIdx isnt partitioned, so just ignore the partition passed and call Load()
	return m.Load(defs)
}

// Load is used to rebuild the index from an existing set of metricDefinitions.
func (m *UnpartitionedMemoryIdx) Load(defs []idx.MetricDefinition) int {
	m.Lock()
	defer m.Unlock()
	var pre time.Time
	var num int
	for i := range defs {
		def := defs[i]
		pre = time.Now()
		if _, ok := m.defById[def.Id]; ok {
			continue
		}

		m.add(createArchive(&def))

		// or after this time.  For metrics that are sent at or close to real time (the typical
		// use case), then the value will be within a couple of seconds of the true lastSave.
		m.defById[def.Id].LastSave = uint32(def.LastUpdate)
		num++
		statAddDuration.Value(time.Since(pre))
	}
	return num
}

func createArchive(def *idx.MetricDefinition) *idx.Archive {
	path := def.NameWithTags()
	schemaId, _ := mdata.MatchSchema(path, def.Interval)
	aggId, _ := mdata.MatchAgg(path)
	irId, _ := IndexRules.Match(path)

	return &idx.Archive{
		MetricDefinition: def,
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

	def := archive.MetricDefinition
	path := def.NameWithTags()

	if TagSupport {
		// Even if there are no tags, index at least "name". It's important to use the definition
		// in the archive pointer that we add to defById, because the pointers must reference the
		// same underlying object in m.defById and m.defByTagSet
		m.indexTags(archive.MetricDefinition)

		if len(def.Tags.KeyValues) > 0 {
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

// Get returns an idx.Archive that matches the supplied schema.MKey.
// Upon failure it returns an empty idx.Archive
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

func (m *UnpartitionedMemoryIdx) TagDetails(orgId uint32, key string, filter *regexp.Regexp, from int64) map[string]uint64 {
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

	keyPtr, err := idx.IdxIntern.GetPtrFromByte([]byte(key))
	if err != nil {
		log.Error("memory-idx: Failed to retrieve interned string for tag key: ", err)
		internError.Inc()
		return nil
	}
	values, ok := tags[keyPtr]
	if !ok {
		return nil
	}

	res := make(map[string]uint64)
	for valuePtr, ids := range values {
		value, err := idx.IdxIntern.GetStringFromPtr(valuePtr)
		if err != nil {
			log.Error("memory-idx: Failed to retrieve interned string for tag value: ", err)
			internError.Inc()
			continue
		}
		if filter != nil && !filter.MatchString(value) {
			continue
		}

		count := uint64(0)
		if from > 0 {
			for id := range ids {
				def, ok := m.defById[id]
				if !ok {
					corruptIndex.Inc()
					log.Errorf("memory-idx: corrupt. ID %q is in tag index but not in the byId lookup table", id)
					continue
				}

				if atomic.LoadInt64(&def.LastUpdate) < from {
					continue
				}

				count++
			}
		} else {
			count += uint64(len(ids))
		}

		if count > 0 {
			res[value] = count
		}
	}

	return res
}

// FindTags returns tags matching the specified conditions
// prefix:      prefix match
// from:        tags must have at least one metric with LastUpdate >= from
// limit:       the maximum number of results to return
//
// the results will always be sorted alphabetically for consistency
func (m *UnpartitionedMemoryIdx) FindTags(orgId uint32, prefix string, from int64, limit uint) []string {
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

	for tagPtr, values := range tags {
		tag, err := idx.IdxIntern.GetStringFromPtr(tagPtr)
		if err != nil {
			log.Error("memory-idx: Failed to retrieve interned string for tag key: ", err)
			internError.Inc()
			continue
		}

		// a tag gets appended to the result set if:
		// either the given prefix is empty or the tag has the given prefix, and
		// either from is set to 0 or the tag has at least one metric with .LastUpdate higher or equal to from
		if (len(prefix) == 0 || strings.HasPrefix(tag, prefix)) && (from == 0 || m.tagHasOneMetricFrom(values, from)) {
			res = append(res, tag)
		}
	}

	sort.Strings(res)

	if len(res) > int(limit) {
		return res[:limit]
	}
	return res
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

	// probably allocating more than necessary, still better than growing
	res := make([]string, 0, len(tags))

	resMap := queryCtx.RunGetTags(tags, m.defById)
	for tag := range resMap {
		if len(prefix) == 0 || strings.HasPrefix(tag, prefix) {
			res = append(res, tag)
		}
	}

	sort.Strings(res)

	if len(res) > int(limit) {
		return res[:limit]
	}
	return res
}

// FindTagValues returns tag values matching the specified conditions
// tag:         tag key match
// prefix:      value prefix match
// from:        tags must have at least one metric with LastUpdate >= from
// limit:       the maximum number of results to return
//
// the results will always be sorted alphabetically for consistency
func (m *UnpartitionedMemoryIdx) FindTagValues(orgId uint32, tag, prefix string, from int64, limit uint) []string {
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

	tagPtr, err := idx.IdxIntern.GetPtrFromByte([]byte(tag))
	if err != nil {
		log.Error("memory-idx: Failed to retrieve uintptr for interned tag key: ", err)
		internError.Inc()
		return nil
	}

	vals, ok := tags[tagPtr]
	if !ok {
		return nil
	}

	res := make([]string, 0, len(vals))
	for valPtr, ids := range vals {
		val, err := idx.IdxIntern.GetStringFromPtr(valPtr)
		if err != nil {
			log.Error("memory-idx: Failed to retrieve interned string for tag value: ", err)
			internError.Inc()
			continue
		}
		if (len(prefix) == 0 || strings.HasPrefix(val, prefix)) && (from == 0 || m.idSetHasOneMetricFrom(ids, from)) {
			res = append(res, val)
		}
	}

	sort.Strings(res)

	if len(res) > int(limit) {
		return res[:limit]
	}
	return res
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

	ids := queryCtx.Run(tags, m.defById)
	valueMap := make(map[string]struct{})
	for id := range ids {
		var ok bool
		var def *idx.Archive
		if def, ok = m.defById[id]; !ok {
			// should never happen because every ID in the tag index
			// must be present in the byId lookup table
			corruptIndex.Inc()
			log.Errorf("memory-idx: ID %q is in tag index but not in the byId lookup table", id)
			continue
		}

		if tag == "name" {
			name := schema.SanitizeNameAsTagValue(def.Name.String())

			if len(prefix) > 0 && !strings.HasPrefix(name, prefix) {
				continue
			}

			valueMap[name] = struct{}{}
		} else {
			for _, t := range def.Tags.KeyValues {
				key, err := idx.IdxIntern.GetStringFromPtr(t.Key)
				if err != nil {
					log.Error("memory-idx: Failed to retrieve interned string for tag key: ", err)
					internError.Inc()
					continue
				}

				if key != tag {
					continue
				}

				value, err := idx.IdxIntern.GetStringFromPtr(t.Value)
				if err != nil {
					log.Error("memory-idx: Failed to retrieve interned string for tag value: ", err)
					internError.Inc()
					continue
				}

				if len(value) > 0 && !strings.HasPrefix(value, prefix) {
					continue
				}

				valueMap[value] = struct{}{}
			}
		}
	}

	res := make([]string, 0, len(valueMap))
	for v := range valueMap {
		res = append(res, v)
	}

	sort.Strings(res)

	if len(res) > int(limit) {
		return res[:limit]
	}
	return res
}

// Tags returns a list of all tag keys associated with the metrics of a given
// organization. The return values are filtered by the regex in the second parameter.
// If the third parameter is >0 then only metrics will be accounted of which the
// LastUpdate time is >= the given value.
func (m *UnpartitionedMemoryIdx) Tags(orgId uint32, filter *regexp.Regexp, from int64) []string {
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

	for tagPtr, values := range tags {
		tag, err := idx.IdxIntern.GetStringFromPtr(tagPtr)
		if err != nil {
			log.Error("memory-idx: Failed to retrieve interned string for tag key: ", err)
			internError.Inc()
			continue
		}

		// filter by pattern if one was given
		if filter != nil && !filter.MatchString(tag) {
			continue
		}

		// if from is > 0 we need to find at least one metric definition where
		// LastUpdate >= from before we add the tag to the result set
		if (from > 0 && m.tagHasOneMetricFrom(values, from)) || from == 0 {
			res = append(res, tag)
		}
	}

	return res
}

func (m *UnpartitionedMemoryIdx) tagHasOneMetricFrom(values TagValues, from int64) bool {
	for _, ids := range values {
		if m.idSetHasOneMetricFrom(ids, from) {
			return true
		}
	}
	return false
}

func (m *UnpartitionedMemoryIdx) idSetHasOneMetricFrom(ids IdSet, from int64) bool {
	for id := range ids {
		def, ok := m.defById[id]
		if !ok {
			corruptIndex.Inc()
			log.Errorf("memory-idx: corrupt. ID %q is in tag index but not in the byId lookup table", id)
			continue
		}

		// as soon as we found one metric definition with LastUpdate >= from
		// we can return true
		if atomic.LoadInt64(&def.LastUpdate) >= from {
			return true
		}
	}

	return false
}

func (m *UnpartitionedMemoryIdx) FindByTag(orgId uint32, query tagquery.Query) []idx.Node {
	if !TagSupport {
		log.Warn("memory-idx: received tag query, but tag support is disabled")
		return nil
	}

	queryCtx := NewTagQueryContext(query)

	m.RLock()
	defer m.RUnlock()

	// construct the output slice of idx.Node's such that there is only 1 idx.Node for each path
	ids := m.idsByTagQuery(orgId, queryCtx)
	byPath := make(map[string]*idx.Node)
	for id := range ids {
		def, ok := m.defById[id]
		if !ok {
			corruptIndex.Inc()
			log.Errorf("memory-idx: corrupt. ID %q has been given, but it is not in the byId lookup table", id)
			continue
		}

		if existing, ok := byPath[def.NameWithTags()]; !ok {
			byPath[def.NameWithTags()] = &idx.Node{
				Path:        def.NameWithTags(),
				Leaf:        true,
				HasChildren: false,
				Defs:        []idx.Archive{CloneArchive(def)},
			}
		} else {
			existing.Defs = append(existing.Defs, CloneArchive(def))
		}
	}

	results := make([]idx.Node, 0, len(byPath))

	for _, v := range byPath {
		results = append(results, *v)
	}

	return results
}

func (m *UnpartitionedMemoryIdx) idsByTagQuery(orgId uint32, query TagQueryContext) IdSet {
	tags, ok := m.tags[orgId]
	if !ok {
		return nil
	}

	return query.Run(tags, m.defById)
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
						log.Debugf("memory-idx: Find: adding to path %s archive id=%s name=%s int=%d schemaId=%d aggId=%d irId=%d lastSave=%d", n.Path, def.Id, def.Name.String(), def.Interval, def.SchemaId, def.AggId, def.IrId, lastSave)
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
	ids := m.idsByTagQuery(orgId, queryCtx)
	m.RUnlock()

	m.Lock()
	defer m.Unlock()
	deleted := m.deleteTaggedByIdSet(orgId, ids)

	// this is a special case where the MetricDefinitions need to be
	// released outside of the normal Delete() path.
	for _, arc := range deleted {
		idx.InternReleaseMetricDefinition(*arc.MetricDefinition)
	}
	return deleted
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
		if !m.deindexTags(tags, def.MetricDefinition) {
			continue
		}
		deletedDefs = append(deletedDefs, CloneArchive(def))
		delete(m.defById, idStr)
	}

	statMetricsActive.DecUint32(uint32(len(deletedDefs)))

	return deletedDefs
}

func (m *UnpartitionedMemoryIdx) DeletePersistent(orgId uint32, pattern string) ([]idx.Archive, error) {
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

func (m *UnpartitionedMemoryIdx) Delete(orgId uint32, pattern string) (int, error) {
	arcs, err := m.DeletePersistent(orgId, pattern)
	return len(arcs), err
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

		if len(def.Tags.KeyValues) == 0 {
			tree, ok := m.tree[def.OrgId]
			if !ok {
				continue DEFS
			}

			n, ok := tree.Items[def.Name.String()]
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
				toPruneTagged[sdef.OrgId][sdef.Id] = struct{}{}
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
