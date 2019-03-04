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

	Enabled             bool
	matchCacheSize      int
	maxPruneLockTime    = time.Millisecond * 100
	maxPruneLockTimeStr string
	TagSupport          bool
	TagQueryWorkers     int // number of workers to spin up when evaluation tag expressions
	indexRulesFile      string
	IndexRules          conf.IndexRules
)

func ConfigSetup() {
	memoryIdx := flag.NewFlagSet("memory-idx", flag.ExitOnError)
	memoryIdx.BoolVar(&Enabled, "enabled", false, "")
	memoryIdx.BoolVar(&TagSupport, "tag-support", false, "enables/disables querying based on tags")
	memoryIdx.IntVar(&TagQueryWorkers, "tag-query-workers", 50, "number of workers to spin up to evaluate tag queries")
	memoryIdx.IntVar(&matchCacheSize, "match-cache-size", 1000, "size of regular expression cache in tag query evaluation")
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
}

type Tree struct {
	Root *Node // the tree starts with a root node.
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

type TagValue map[string]IdSet    // value -> set of ids
type TagIndex map[string]TagValue // key -> list of values

func (t *TagIndex) addTagId(name, value string, id schema.MKey) {
	ti := *t
	if _, ok := ti[name]; !ok {
		ti[name] = make(TagValue)
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
	Name     string
	Parent   *Node
	Children map[string]*Node
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
		return fmt.Sprintf("leaf - %s", n.Path())
	}
	return fmt.Sprintf("branch - %s", n.Path())
}

func (n *Node) Path() string {
	if n.Parent == nil {
		return ""
	}
	p := n.Parent.Path()
	if p == "" {
		return n.Name
	}
	return fmt.Sprintf("%s.%s", p, n.Name)
}

func (n *Node) Find(path []string) *Node {
	if len(path) == 0 {
		return n
	}
	if child, ok := n.Children[path[0]]; ok {
		if len(path) == 1 {
			return child
		}
		return child.Find(path[1:])
	}

	// path not found.
	return nil
}

// Implements the the "MetricIndex" interface
type MemoryIdx struct {
	sync.RWMutex

	// used for both hierarchy and tag index, so includes all MDs, with
	// and without tags. It also mixes all orgs into one flat map.
	defById map[schema.MKey]*idx.Archive

	// used by hierarchy index only
	tree map[uint32]*Tree // by orgId

	// used by tag index
	defByTagSet defByTagSet
	tags        map[uint32]TagIndex // by orgId
}

func New() *MemoryIdx {
	return &MemoryIdx{
		defById:     make(map[schema.MKey]*idx.Archive),
		defByTagSet: make(defByTagSet),
		tree:        make(map[uint32]*Tree),
		tags:        make(map[uint32]TagIndex),
	}
}

func (m *MemoryIdx) Init() error {
	return nil
}

func (m *MemoryIdx) Stop() {
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

// Update updates an existing archive, if found.
// It returns whether it was found, and - if so - the (updated) existing archive and its old partition
func (m *MemoryIdx) Update(point schema.MetricPoint, partition int32) (idx.Archive, int32, bool) {
	pre := time.Now()

	m.RLock()
	defer m.RUnlock()

	existing, ok := m.defById[point.MKey]
	if ok {
		log.Debugf("memory-idx: metricDef with id %v already in index", point.MKey)

		bumpLastUpdate(&existing.LastUpdate, int64(point.Time))

		oldPart := atomic.SwapInt32(&existing.Partition, partition)
		statUpdate.Inc()
		statUpdateDuration.Value(time.Since(pre))
		return *existing, oldPart, true
	}

	return idx.Archive{}, 0, false
}

// AddOrUpdate returns the corresponding Archive for the MetricData.
// if it is existing -> updates lastUpdate based on .Time, and partition
// if was new        -> adds new MetricDefinition to index
func (m *MemoryIdx) AddOrUpdate(mkey schema.MKey, data *schema.MetricData, partition int32) (idx.Archive, int32, bool) {
	pre := time.Now()

	// Optimistically read lock
	m.RLock()

	existing, ok := m.defById[mkey]
	if ok {
		log.Debugf("memory-idx: metricDef with id %s already in index.", mkey)
		bumpLastUpdate(&existing.LastUpdate, data.Time)
		oldPart := atomic.SwapInt32(&existing.Partition, partition)
		statUpdate.Inc()
		statUpdateDuration.Value(time.Since(pre))
		m.RUnlock()
		return *existing, oldPart, ok
	}

	m.RUnlock()
	m.Lock()
	defer m.Unlock()

	def := schema.MetricDefinitionFromMetricData(data)
	def.Partition = partition
	archive := m.add(def)
	statMetricsActive.Inc()
	statAddDuration.Value(time.Since(pre))

	if TagSupport {
		m.indexTags(def)
	}

	return archive, 0, false
}

// UpdateArchive updates the archive information
func (m *MemoryIdx) UpdateArchive(archive idx.Archive) {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.defById[archive.Id]; !ok {
		return
	}
	*(m.defById[archive.Id]) = archive
}

// indexTags reads the tags of a given metric definition and creates the
// corresponding tag index entries to refer to it. It assumes a lock is
// already held.
func (m *MemoryIdx) indexTags(def *schema.MetricDefinition) {
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
	tags.addTagId("name", def.Name, def.Id)

	m.defByTagSet.add(def)
}

// deindexTags takes a given metric definition and removes all references
// to it from the tag index. It assumes a lock is already held.
// a return value of "false" means there was an error and the deindexing was
// unsuccessful, "true" means the indexing was at least partially or completely
// successful
func (m *MemoryIdx) deindexTags(tags TagIndex, def *schema.MetricDefinition) bool {
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

	tags.delTagId("name", def.Name, def.Id)

	m.defByTagSet.del(def)

	return true
}

// Used to rebuild the index from an existing set of metricDefinitions.
func (m *MemoryIdx) Load(defs []schema.MetricDefinition) int {
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

		m.add(def)

		if TagSupport {
			m.indexTags(def)
		}

		// as we are loading the metricDefs from a persistent store, set the lastSave
		// to the lastUpdate timestamp.  This won't exactly match the true lastSave Timstamp,
		// but it will be close enough and it will always be true that the lastSave was at
		// or after this time.  For metrics that are sent at or close to real time (the typical
		// use case), then the value will be within a couple of seconds of the true lastSave.
		m.defById[def.Id].LastSave = uint32(def.LastUpdate)
		num++
		statMetricsActive.Inc()
		statAddDuration.Value(time.Since(pre))
	}
	return num
}

func MetricDefPathSlice(def *schema.MetricDefinition) []string {
	name := def.Name
	path := []string{}
	pos := 0
	last := 0
	for pos = strings.Index(name[last:], "."); pos >= 0; pos = strings.Index(name[last:], ".") {
		path = append(path, name[last:last+pos])
		last = last + pos + 1

	}
	if len(def.Tags) > 0 {
		path = append(path, def.NameWithTags()[last:])
	} else {
		path = append(path, name[last:])
	}
	return path
}

func (m *MemoryIdx) add(def *schema.MetricDefinition) idx.Archive {
	path := def.NameWithTags()
	pathSlice := MetricDefPathSlice(def)
	schemaId, _ := mdata.MatchSchema(path, def.Interval)
	aggId, _ := mdata.MatchAgg(path)
	irId, _ := IndexRules.Match(path)

	archive := &idx.Archive{
		MetricDefinition: *def,
		SchemaId:         schemaId,
		AggId:            aggId,
		IrId:             irId,
	}

	if TagSupport && len(def.Tags) > 0 {
		if _, ok := m.defById[def.Id]; !ok {
			m.defById[def.Id] = archive
			statAdd.Inc()
			log.Debugf("memory-idx: adding %s to DefById", path)
		}
		return *archive
	}

	//first check to see if a tree has been created for this OrgId
	tree, ok := m.tree[def.OrgId]
	if !ok {
		log.Debugf("memory-idx: first metricDef seen for orgId %d", def.OrgId)
		m.tree[def.OrgId] = &Tree{
			Root: &Node{
				Name:     "",
				Parent:   nil,
				Children: make(map[string]*Node),
				Defs:     make([]schema.MKey, 0, 1),
			},
		}
		tree = m.tree[def.OrgId]
	}

	current := tree.Root

	for i := range pathSlice {
		p := pathSlice[i]
		next, ok := current.Children[p]
		if !ok {
			next = &Node{
				Name:     p,
				Parent:   current,
				Children: make(map[string]*Node),
				Defs:     make([]schema.MKey, 0, 1),
			}
			current.Children[p] = next
		}
		current = next
	}

	current.Defs = append(current.Defs, def.Id)
	m.defById[def.Id] = archive
	statAdd.Inc()

	return *archive
}

func (m *MemoryIdx) Get(id schema.MKey) (idx.Archive, bool) {
	pre := time.Now()
	m.RLock()
	defer m.RUnlock()
	def, ok := m.defById[id]
	statGetDuration.Value(time.Since(pre))
	if ok {
		return *def, ok
	}
	return idx.Archive{}, ok
}

// GetPath returns the node under the given org and path.
// this is an alternative to Find for when you have a path, not a pattern, and want to lookup in a specific org tree only.
func (m *MemoryIdx) GetPath(orgId uint32, path string) []idx.Archive {
	m.RLock()
	defer m.RUnlock()
	tree, ok := m.tree[orgId]
	if !ok {
		return nil
	}
	node := tree.Root.Find(strings.Split(path, "."))
	if node == nil {
		return nil
	}
	archives := make([]idx.Archive, len(node.Defs))
	for i, def := range node.Defs {
		archive := m.defById[def]
		archives[i] = *archive
	}
	return archives
}

func (m *MemoryIdx) TagDetails(orgId uint32, key, filter string, from int64) (map[string]uint64, error) {
	if !TagSupport {
		log.Warn("memory-idx: received tag query, but tag support is disabled")
		return nil, nil
	}

	var re *regexp.Regexp
	if len(filter) > 0 {
		if filter[0] != byte('^') {
			filter = "^(?:" + filter + ")"
		}
		var err error
		re, err = regexp.Compile(filter)
		if err != nil {
			return nil, err
		}
	}

	m.RLock()
	defer m.RUnlock()

	tags, ok := m.tags[orgId]
	if !ok {
		return nil, nil
	}

	values, ok := tags[key]
	if !ok {
		return nil, nil
	}

	res := make(map[string]uint64)
	for value, ids := range values {
		if re != nil && !re.MatchString(value) {
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

	return res, nil
}

// FindTags returns tags matching the specified conditions
// prefix:      prefix match
// expressions: tagdb expressions in the same format as graphite
// from:        tags must have at least one metric with LastUpdate >= from
// limit:       the maximum number of results to return
//
// the results will always be sorted alphabetically for consistency
func (m *MemoryIdx) FindTags(orgId uint32, prefix string, expressions []string, from int64, limit uint) ([]string, error) {
	if !TagSupport {
		log.Warn("memory-idx: received tag query, but tag support is disabled")
		return nil, nil
	}
	var res []string

	// only if expressions are specified we need to build a tag query.
	// otherwise, the generation of the result set is much simpler
	if len(expressions) > 0 {
		// incorporate the tag prefix into the tag query expressions
		if len(prefix) > 0 {
			expressions = append(expressions, "__tag^="+prefix)
		}

		query, err := NewTagQuery(expressions, from)
		if err != nil {
			return nil, err
		}

		// only acquire lock after we're sure the query is valid
		m.RLock()
		defer m.RUnlock()

		tags, ok := m.tags[orgId]
		if !ok {
			return nil, nil
		}

		resMap := query.RunGetTags(tags, m.defById)
		for tag := range resMap {
			res = append(res, tag)
		}

		sort.Strings(res)
		if uint(len(res)) > limit {
			res = res[:limit]
		}
	} else {
		m.RLock()
		defer m.RUnlock()

		tags, ok := m.tags[orgId]
		if !ok {
			return nil, nil
		}

		tagsSorted := make([]string, 0, len(tags))
		for tag := range tags {
			if !strings.HasPrefix(tag, prefix) {
				continue
			}

			tagsSorted = append(tagsSorted, tag)
		}

		sort.Strings(tagsSorted)

		for _, tag := range tagsSorted {
			// only if from is specified we need to find at least one
			// metric with LastUpdate >= from
			if (from > 0 && m.hasOneMetricFrom(tags, tag, from)) || from == 0 {
				res = append(res, tag)
			}

			// the tags are processed in sorted order, so once we have have "limit" results we can break
			if uint(len(res)) >= limit {
				break
			}
		}
	}

	return res, nil
}

// FindTagValues returns tag values matching the specified conditions
// tag:         tag key match
// prefix:      value prefix match
// expressions: tagdb expressions in the same format as graphite
// from:        tags must have at least one metric with LastUpdate >= from
// limit:       the maximum number of results to return
//
// the results will always be sorted alphabetically for consistency
func (m *MemoryIdx) FindTagValues(orgId uint32, tag, prefix string, expressions []string, from int64, limit uint) ([]string, error) {
	if !TagSupport {
		log.Warn("memory-idx: received tag query, but tag support is disabled")
		return nil, nil
	}
	var res []string

	// only if expressions are specified we need to build a tag query.
	// otherwise, the generation of the result set is much simpler
	if len(expressions) > 0 {

		// add the value prefix into the expressions as an additional condition
		if len(prefix) > 0 {
			expressions = append(expressions, tag+"^="+prefix)
		} else {
			// if no value prefix has been specified we still require that at
			// least the given tag must be present
			expressions = append(expressions, tag+"!=")
		}

		query, err := NewTagQuery(expressions, from)
		if err != nil {
			return nil, err
		}

		// only acquire lock after we're sure the query is valid
		m.RLock()
		defer m.RUnlock()

		tags, ok := m.tags[orgId]
		if !ok {
			return nil, nil
		}

		ids := query.Run(tags, m.defById)
		valueMap := make(map[string]struct{})
		prefix := tag + "="
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

			// special case if the tag to complete values for is "name"
			if tag == "name" {
				valueMap[def.Name] = struct{}{}
			} else {
				for _, t := range def.Tags {
					if !strings.HasPrefix(t, prefix) {
						continue
					}

					// keep the value after "=", that's why "+1"
					valueMap[t[len(prefix):]] = struct{}{}
				}
			}
		}

		res = make([]string, 0, len(valueMap))
		for v := range valueMap {
			res = append(res, v)
		}
	} else {
		m.RLock()
		defer m.RUnlock()

		tags, ok := m.tags[orgId]
		if !ok {
			return nil, nil
		}

		vals, ok := tags[tag]
		if !ok {
			return nil, nil
		}

		res = make([]string, 0, len(vals))
		for val := range vals {
			if !strings.HasPrefix(val, prefix) {
				continue
			}

			res = append(res, val)
		}
	}

	sort.Strings(res)
	if uint(len(res)) > limit {
		res = res[:limit]
	}

	return res, nil
}

// Tags returns a list of all tag keys associated with the metrics of a given
// organization. The return values are filtered by the regex in the second parameter.
// If the third parameter is >0 then only metrics will be accounted of which the
// LastUpdate time is >= the given value.
func (m *MemoryIdx) Tags(orgId uint32, filter string, from int64) ([]string, error) {
	if !TagSupport {
		log.Warn("memory-idx: received tag query, but tag support is disabled")
		return nil, nil
	}

	var re *regexp.Regexp
	if len(filter) > 0 {
		if filter[0] != byte('^') {
			filter = "^(?:" + filter + ")"
		}
		var err error
		re, err = regexp.Compile(filter)
		if err != nil {
			return nil, err
		}
	}

	m.RLock()
	defer m.RUnlock()

	tags, ok := m.tags[orgId]
	if !ok {
		return nil, nil
	}

	var res []string

	// if there is no filter/from given we know how much space we'll need
	// and can preallocate it
	if re == nil && from == 0 {
		res = make([]string, 0, len(tags))
	}

	for tag := range tags {
		// filter by pattern if one was given
		if re != nil && !re.MatchString(tag) {
			continue
		}

		// if from is > 0 we need to find at least one metric definition where
		// LastUpdate >= from before we add the tag to the result set
		if (from > 0 && m.hasOneMetricFrom(tags, tag, from)) || from == 0 {
			res = append(res, tag)
		}
	}

	return res, nil
}

func (m *MemoryIdx) hasOneMetricFrom(tags TagIndex, tag string, from int64) bool {
	for _, ids := range tags[tag] {
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
	}
	return false
}

func (m *MemoryIdx) FindByTag(orgId uint32, expressions []string, from int64) ([]idx.Node, error) {
	if !TagSupport {
		log.Warn("memory-idx: received tag query, but tag support is disabled")
		return nil, nil
	}

	query, err := NewTagQuery(expressions, from)
	if err != nil {
		return nil, err
	}

	m.RLock()
	defer m.RUnlock()

	// construct the output slice of idx.Node's such that there is only 1 idx.Node for each path
	ids := m.idsByTagQuery(orgId, query)
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
				Defs:        []idx.Archive{*def},
			}
		} else {
			existing.Defs = append(existing.Defs, *def)
		}
	}

	results := make([]idx.Node, 0, len(byPath))

	for _, v := range byPath {
		results = append(results, *v)
	}

	return results, nil
}

func (m *MemoryIdx) idsByTagQuery(orgId uint32, query TagQuery) IdSet {
	tags, ok := m.tags[orgId]
	if !ok {
		return nil
	}

	return query.Run(tags, m.defById)
}

func (m *MemoryIdx) Find(orgId uint32, pattern string, from int64) ([]idx.Node, error) {
	pre := time.Now()
	m.RLock()
	defer m.RUnlock()
	matchedNodes, err := m.find(orgId, pattern)
	if err != nil {
		return nil, err
	}
	if orgId != idx.OrgIdPublic && idx.OrgIdPublic > 0 {
		publicNodes, err := m.find(idx.OrgIdPublic, pattern)
		if err != nil {
			return nil, err
		}
		matchedNodes = append(matchedNodes, publicNodes...)
	}
	log.Debugf("memory-idx: %d nodes matching pattern %s found", len(matchedNodes), pattern)
	results := make([]idx.Node, 0)
	byPath := make(map[string]struct{})
	// construct the output slice of idx.Node's such that there is only 1 idx.Node
	// for each path, and it holds all defs that the Node refers too.
	// if there are public (orgId OrgIdPublic) and private leaf nodes with the same series
	// path, then the public metricDefs will be excluded.
	for _, n := range matchedNodes {
		path := n.Path()
		if _, ok := byPath[path]; !ok {
			idxNode := idx.Node{
				Path:        path,
				Leaf:        n.Leaf(),
				HasChildren: n.HasChildren(),
			}
			if idxNode.Leaf {
				idxNode.Defs = make([]idx.Archive, 0, len(n.Defs))
				for _, id := range n.Defs {
					def := m.defById[id]
					if from != 0 && atomic.LoadInt64(&def.LastUpdate) < from {
						statFiltered.Inc()
						log.Debugf("memory-idx: from is %d, so skipping %s which has LastUpdate %d", from, def.Id, atomic.LoadInt64(&def.LastUpdate))
						continue
					}
					log.Debugf("memory-idx: Find: adding to path %s archive id=%s name=%s int=%d schemaId=%d aggId=%d irId=%d lastSave=%d", path, def.Id, def.Name, def.Interval, def.SchemaId, def.AggId, def.IrId, def.LastSave)
					idxNode.Defs = append(idxNode.Defs, *def)
				}
				if len(idxNode.Defs) == 0 {
					continue
				}
			}
			results = append(results, idxNode)
			byPath[path] = struct{}{}
		} else {
			log.Debugf("memory-idx: path %s already seen", path)
		}
	}
	log.Debugf("memory-idx: %d nodes has %d unique paths.", len(matchedNodes), len(results))
	statFindDuration.Value(time.Since(pre))
	return results, nil
}

// find returns all Nodes matching the pattern for the given orgId
func (m *MemoryIdx) find(orgId uint32, pattern string) ([]*Node, error) {
	tree, ok := m.tree[orgId]
	if !ok {
		log.Debugf("memory-idx: orgId %d has no metrics indexed.", orgId)
		return nil, nil
	}

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

	log.Debugf("memory-idx: starting search at orgId %d, node %q", orgId, nodes[:pos])
	startNode := tree.Root.Find(nodes[:pos])

	if startNode == nil {
		log.Debugf("memory-idx: branch %q does not exist in the index for orgId %d", nodes[:pos], orgId)
		return nil, nil
	}

	if pos == len(nodes) {
		return []*Node{startNode}, nil
	}

	children := make(map[string][]*Node)
	for name, n := range startNode.Children {
		children[name] = []*Node{n}
	}

	for i := pos; i < len(nodes); i++ {
		p := nodes[i]

		matcher, err := getMatcher(p)
		if err != nil {
			return nil, err
		}
		children = matcher(children)

		if len(children) == 0 {
			log.Debug("memory-idx: pattern does not match any series.")
			break
		}
		if i == len(nodes)-1 {
			break
		}
		grandChildren := map[string][]*Node{}
		for _, c := range children {
			for _, child := range c {
				for grandChildName, grandChild := range child.Children {
					_, ok := grandChildren[grandChildName]
					if !ok {
						grandChildren[grandChildName] = []*Node{grandChild}
					} else {
						grandChildren[grandChildName] = append(grandChildren[grandChildName], grandChild)
					}
				}
			}
		}
		children = grandChildren
	}

	log.Debugf("memory-idx: reached pattern length. %d nodes matched", len(children))
	result := make([]*Node, 0, len(children))
	for _, n := range children {
		result = append(result, n...)
	}
	return result, nil
}

func (m *MemoryIdx) List(orgId uint32) []idx.Archive {
	pre := time.Now()
	m.RLock()
	defer m.RUnlock()

	defs := make([]idx.Archive, 0)
	for _, def := range m.defById {
		if def.OrgId == orgId || def.OrgId == idx.OrgIdPublic {
			defs = append(defs, *def)
		}
	}

	statListDuration.Value(time.Since(pre))

	return defs
}

func (m *MemoryIdx) DeleteTagged(orgId uint32, paths []string) ([]idx.Archive, error) {
	if !TagSupport {
		log.Warn("memory-idx: received tag query, but tag support is disabled")
		return nil, nil
	}

	queries := make([]TagQuery, 0, len(paths))
	for _, path := range paths {
		elements := strings.Split(path, ";")
		if len(elements) < 2 {
			continue
		}
		expressions := elements[1:]
		expressions = append(expressions, "name="+elements[0])

		q, err := NewTagQuery(expressions, 0)
		if err != nil {
			return nil, err
		}
		queries = append(queries, q)
	}

	ids := make(IdSet)
	for _, query := range queries {
		m.RLock()
		queryIds := m.idsByTagQuery(orgId, query)
		m.RUnlock()

		for id := range queryIds {
			ids[id] = struct{}{}
		}
	}

	m.Lock()
	defer m.Unlock()
	return m.deleteTaggedByIdSet(orgId, ids), nil
}

// deleteTaggedByIdSet deletes a map of ids from the tag index and also the DefByIds
// it is important that only IDs of series with tags get passed in here, because
// otherwise the result might be inconsistencies between DefByIDs and the tree index.
func (m *MemoryIdx) deleteTaggedByIdSet(orgId uint32, ids IdSet) []idx.Archive {
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
		deletedDefs = append(deletedDefs, *def)
		delete(m.defById, idStr)
	}

	statMetricsActive.Set(len(m.defById))

	return deletedDefs
}

func (m *MemoryIdx) Delete(orgId uint32, pattern string) ([]idx.Archive, error) {
	var deletedDefs []idx.Archive
	pre := time.Now()
	m.Lock()
	defer m.Unlock()
	found, err := m.find(orgId, pattern)
	if err != nil {
		return nil, err
	}

	for _, f := range found {
		deleted := m.delete(orgId, f, true, true)
		deletedDefs = append(deletedDefs, deleted...)
	}

	statMetricsActive.Set(len(m.defById))
	statDeleteDuration.Value(time.Since(pre))

	return deletedDefs, nil
}

func (m *MemoryIdx) delete(orgId uint32, n *Node, deleteEmptyParents, deleteChildren bool) []idx.Archive {
	deletedDefs := make([]idx.Archive, 0)
	if deleteChildren && n.HasChildren() {
		log.Debugf("memory-idx: deleting branch %s", n.Name)
		// walk up the tree to find all leaf nodes and delete them.
		for name, child := range n.Children {
			log.Debugf("memory-idx: deleting child %s from branch %s", name, n.Name)
			deleted := m.delete(orgId, child, false, true)
			deletedDefs = append(deletedDefs, deleted...)
		}
		n.Children = map[string]*Node{}
	}

	// delete the metricDefs
	for _, id := range n.Defs {
		log.Debugf("memory-idx: deleting %s from index", id)
		deletedDefs = append(deletedDefs, *m.defById[id])
		delete(m.defById, id)
	}

	n.Defs = nil

	if !deleteChildren && n.HasChildren() {
		return deletedDefs
	}

	// de-link the node from the parent
	delete(n.Parent.Children, n.Name)

	if !deleteEmptyParents {
		return deletedDefs
	}

	// walk up the tree and remove empty nodes.
	current := n.Parent
	for current != nil {
		if current.HasChildren() || current.Leaf() || current.Parent == nil {
			break
		}
		// de-link the node from the parent
		delete(current.Parent.Children, current.Name)
		current = current.Parent
	}

	return deletedDefs
}

// Prune prunes series from the index if they have become stale per their index-rule
func (m *MemoryIdx) Prune(now time.Time) ([]idx.Archive, error) {
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
	toPruneUntagged := make(map[uint32][]*Node, len(orgs))
	toPruneTagged := make(map[uint32]IdSet, len(orgs))
	for org := range orgs {
		toPruneTagged[org] = make(IdSet)
		toPruneUntagged[org] = make([]*Node, 0)
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

			// we should only prune defs if all defs with the same name are old.
			n := tree.Root.Find(strings.Split(def.Name, "."))
			if n == nil || !n.Leaf() {
				continue DEFS
			}

			for _, id := range n.Defs {
				if atomic.LoadInt64(&m.defById[id].LastUpdate) >= cutoff {
					continue DEFS
				}
			}

			toPruneUntagged[def.OrgId] = append(toPruneUntagged[def.OrgId], n)
		} else {
			defs := m.defByTagSet.defs(def.OrgId, def.NameWithTags())
			// if any other MetricDef with the same tag set is not expired yet,
			// then we do not want to prune any of them
			for def := range defs {
				if atomic.LoadInt64(&def.LastUpdate) >= cutoff {
					continue DEFS
				}
			}

			for def := range defs {
				toPruneTagged[def.OrgId][def.Id] = struct{}{}
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

	for org, nodes := range toPruneUntagged {
		if len(nodes) == 0 {
			continue
		}

		for _, n := range nodes {
			tl.Wait()
			lockStart := time.Now()
			m.Lock()

			log.Debugf("memory-idx: series %s for orgId:%d is stale. pruning it.", n.Path(), org)
			defs := m.delete(org, n, true, false)
			m.Unlock()
			tl.Add(time.Since(lockStart))
			pruned = append(pruned, defs...)
		}
	}

	statMetricsActive.Set(len(m.defById))

	duration := time.Since(pre)
	log.Infof("memory-idx: finished pruning of %d series in %s", len(pruned), duration)

	statPruneDuration.Value(duration)
	return pruned, nil
}

func getMatcher(path string) (func(map[string][]*Node) map[string][]*Node, error) {
	// Matches everything
	if path == "*" {
		return func(children map[string][]*Node) map[string][]*Node {
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

		return func(children map[string][]*Node) map[string][]*Node {
			matches := map[string][]*Node{}
			for _, r := range regexes {
				for name, n := range children {
					if r.MatchString(name) {
						log.Debugf("memory-idx: %s =~ %s", name, r.String())
						_, ok := matches[name]
						if !ok {
							matches[name] = n
						} else {
							matches[name] = append(matches[name], n...)
						}
					}
				}
			}
			return matches
		}, nil
	}

	// Exact match one or more values
	return func(children map[string][]*Node) map[string][]*Node {
		results := map[string][]*Node{}
		for _, p := range patterns {
			if n, ok := children[p]; ok {
				log.Debugf("memory-idx: %s matches %s", p, n[0].Name)
				results[n[0].Name] = n
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
