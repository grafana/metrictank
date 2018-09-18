package memory

import (
	"flag"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grafana/metrictank/errors"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/schema"
	"github.com/rakyll/globalconf"
	log "github.com/sirupsen/logrus"
)

var (
	LogLevel int

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

	Enabled         bool
	matchCacheSize  int
	TagSupport      bool
	TagQueryWorkers int // number of workers to spin up when evaluation tag expressions
)

func ConfigSetup() {
	memoryIdx := flag.NewFlagSet("memory-idx", flag.ExitOnError)
	memoryIdx.BoolVar(&Enabled, "enabled", false, "")
	memoryIdx.BoolVar(&TagSupport, "tag-support", false, "enables/disables querying based on tags")
	memoryIdx.IntVar(&TagQueryWorkers, "tag-query-workers", 50, "number of workers to spin up to evaluate tag queries")
	memoryIdx.IntVar(&matchCacheSize, "match-cache-size", 1000, "size of regular expression cache in tag query evaluation")
	globalconf.Register("memory-idx", memoryIdx)
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
	Path     string
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
		log.WithFields(log.Fields{
			"id": point.MKey,
		}).Debug("memory-idx: metricDef with id already in index")

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
		log.WithFields(log.Fields{
			"metricdef.id": mkey,
		}).Debug("memory-idx: metricDef with id already in index")
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
			log.WithFields(log.Fields{
				"tag": tag,
				"id":  def.Id,
			}).Error("memory-idx: Tag has an invalid format")
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
			log.WithFields(log.Fields{
				"tag": tag,
				"id":  def.Id,
			}).Error("memory-idx: Tag has an invalid format")
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
		// to the lastUpdate timestamp.  This wont exactly match the true lastSave Timstamp,
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

func (m *MemoryIdx) add(def *schema.MetricDefinition) idx.Archive {
	path := def.NameWithTags()

	schemaId, _ := mdata.MatchSchema(path, def.Interval)
	aggId, _ := mdata.MatchAgg(path)
	sort.Strings(def.Tags)
	archive := &idx.Archive{
		MetricDefinition: *def,
		SchemaId:         schemaId,
		AggId:            aggId,
	}

	if TagSupport && len(def.Tags) > 0 {
		if _, ok := m.defById[def.Id]; !ok {
			m.defById[def.Id] = archive
			statAdd.Inc()
			log.WithFields(log.Fields{
				"path": path,
			}).Debug("memory-idx: adding entry to DefById")
		}
		return *archive
	}

	//first check to see if a tree has been created for this OrgId
	tree, ok := m.tree[def.OrgId]
	if !ok || len(tree.Items) == 0 {
		log.WithFields(log.Fields{
			"org.id": def.OrgId,
		}).Debug("memory-idx: first metricDef seen for orgId")
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
			log.WithFields(log.Fields{
				"path": path,
				"id":   def.Id,
			}).Debug("memory-idx: existing index entry, adding id to Defs list")
			node.Defs = append(node.Defs, def.Id)
			m.defById[def.Id] = archive
			statAdd.Inc()
			return *archive
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
			log.WithFields(log.Fields{
				"child.node":  prevNode,
				"parent.node": n.Path,
			}).Debug("memory-idx: adding child node")
			n.Children = append(n.Children, prevNode)
			break
		}

		log.WithFields(log.Fields{
			"branch":     branch,
			"child.node": prevNode,
		}).Debug("memory-idx: creating branch with child")
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
		log.WithFields(log.Fields{
			"branch": branch,
		}).Debug("memory-idx: no existing branches found, adding to the root node")
		n := tree.Items[""]
		n.Children = append(n.Children, branch)
	}

	// Add leaf node
	log.WithFields(log.Fields{
		"leaf": path,
	}).Debug("memory-idx: creating leaf")
	tree.Items[path] = &Node{
		Path:     path,
		Children: []string{},
		Defs:     []schema.MKey{def.Id},
	}
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
	node := tree.Items[path]
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
					log.WithFields(log.Fields{
						"id": id,
					}).Error("memory-idx: corrupt, ID is in tag index but not in the byId lookup table")
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
				log.WithFields(log.Fields{
					"id": id,
				}).Error("memory-idx: ID is in tag index but not in the byId lookup table")
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
				log.WithFields(log.Fields{
					"id": id,
				}).Error("memory-idx: corrupt, ID is in tag index but not in the byId lookup table")
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
			log.WithFields(log.Fields{
				"id": id,
			}).Error("memory-idx: corrupt, ID has been given, but it is not in the byId lookup table")
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
	log.WithFields(log.Fields{
		"num.nodes": len(matchedNodes),
		"pattern":   pattern,
	}).Error("memory-idx: found nodes matching pattern")
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
					if from != 0 && atomic.LoadInt64(&def.LastUpdate) < from {
						statFiltered.Inc()
						log.WithFields(log.Fields{
							"from":        from,
							"id":          def.Id,
							"last.update": atomic.LoadInt64(&def.LastUpdate),
						}).Debug("memory-idx: skipping stale id")
						continue
					}
					log.WithFields(log.Fields{
						"path":       n.Path,
						"archive.id": def.Id,
						"name":       def.Name,
						"interval":   def.Interval,
						"schema.id":  def.SchemaId,
						"agg.id":     def.AggId,
						"last.save":  def.LastSave,
					}).Debug("memory-idx Find: adding")
					idxNode.Defs = append(idxNode.Defs, *def)
				}
				if len(idxNode.Defs) == 0 {
					continue
				}
			}
			results = append(results, idxNode)
			byPath[n.Path] = struct{}{}
		} else {
			log.WithFields(log.Fields{
				"path": n.Path,
			}).Error("memory-idx: path already seen")
		}
	}
	log.WithFields(log.Fields{
		"num.nodes":    len(matchedNodes),
		"unique.paths": len(results),
	}).Error("memory-idx: nodes have unique paths")
	statFindDuration.Value(time.Since(pre))
	return results, nil
}

// find returns all Nodes matching the pattern for the given orgId
func (m *MemoryIdx) find(orgId uint32, pattern string) ([]*Node, error) {
	tree, ok := m.tree[orgId]
	if !ok {
		log.WithFields(log.Fields{
			"org.id": orgId,
		}).Error("memory-idx: no metrics indexed for orgId")
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
			log.WithFields(log.Fields{
				"node":     nodes[i],
				"position": i,
			}).Error("memory-idx: found first pattern sequence at node")
			pos = i
			break
		}
	}
	var branch string
	if pos != 0 {
		branch = strings.Join(nodes[:pos], ".")
	}
	log.WithFields(log.Fields{
		"org.id": orgId,
		"node":   branch,
	}).Error("memory-idx: starting search")
	startNode, ok := tree.Items[branch]

	if !ok {
		log.WithFields(log.Fields{
			"org.id": orgId,
			"branch": branch,
		}).Debug("memory-idx: branch does not exist in the index")
		return nil, nil
	}

	if startNode == nil {
		corruptIndex.Inc()
		log.WithFields(log.Fields{
			"org.id":   orgId,
			"pattern":  pattern,
			"position": pos,
			"branch":   branch,
		}).Error("memory-idx: startNode is nil")
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
				log.WithFields(log.Fields{
					"path":    c.Path,
					"pattern": pattern,
				}).Error("memory-idx: end of branch reached with no match found")
				// expecting a branch
				continue
			}
			log.WithFields(log.Fields{
				"num.children": len(c.Children),
				"path":         c.Path,
				"pattern":      nodes[i],
			}).Error("memory-idx: searching children for match")
			matches := matcher(c.Children)
			for _, m := range matches {
				newBranch := c.Path + "." + m
				if c.Path == "" {
					newBranch = m
				}
				grandChild := tree.Items[newBranch]
				if grandChild == nil {
					corruptIndex.Inc()
					log.WithFields(log.Fields{
						"org.id":   orgId,
						"pattern":  pattern,
						"position": pos,
						"p":        p,
						"path":     newBranch,
					}).Error("memory-idx: grandChild is nil")
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

	log.WithFields(log.Fields{
		"num.matched.nodes": len(children),
	}).Error("memory-idx: reached pattern length")
	return children, nil
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

	statMetricsActive.Add(-1 * len(ids))

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
		statMetricsActive.DecUint32(uint32(len(deleted)))
		deletedDefs = append(deletedDefs, deleted...)
	}
	statDeleteDuration.Value(time.Since(pre))

	return deletedDefs, nil
}

func (m *MemoryIdx) delete(orgId uint32, n *Node, deleteEmptyParents, deleteChildren bool) []idx.Archive {
	tree := m.tree[orgId]
	deletedDefs := make([]idx.Archive, 0)
	if deleteChildren && n.HasChildren() {
		log.WithFields(log.Fields{
			"branch": n.Path,
		}).Error("memory-idx: deleting branch")
		// walk up the tree to find all leaf nodes and delete them.
		for _, child := range n.Children {
			node, ok := tree.Items[n.Path+"."+child]
			if !ok {
				corruptIndex.Inc()
				log.WithFields(log.Fields{
					"node": n.Path + "." + child,
				}).Error("memory-idx: node missing, Index is corrupt")
				continue
			}
			log.WithFields(log.Fields{
				"child":  node.Path,
				"branch": n.Path,
			}).Error("memory-idx: deleting child from branch")
			deleted := m.delete(orgId, node, false, true)
			deletedDefs = append(deletedDefs, deleted...)
		}
		n.Children = nil
	}

	// delete the metricDefs
	for _, id := range n.Defs {
		log.WithFields(log.Fields{
			"id": id,
		}).Debug("memory-idx: deleting id from index")
		deletedDefs = append(deletedDefs, *m.defById[id])
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
		log.WithFields(log.Fields{
			"node":   nodes[i],
			"branch": branch,
		}).Error("memory-idx: removing node from branch")
		bNode, ok := tree.Items[branch]
		if !ok {
			corruptIndex.Inc()
			log.WithFields(log.Fields{
				"node": branch,
			}).Error("memory-idx: node missing, Index is corrupt")
			continue
		}
		if len(bNode.Children) > 1 {
			newChildren := make([]string, 0, len(bNode.Children)-1)
			for _, child := range bNode.Children {
				if child != nodes[i] {
					newChildren = append(newChildren, child)
				} else {
					log.WithFields(log.Fields{
						"child":  child,
						"branch": bNode.Path,
					}).Debug("memory-idx: child removed from branch")
				}
			}
			bNode.Children = newChildren
			log.WithFields(log.Fields{
				"branch": bNode.Path,
			}).Error("memory-idx: branch has other children, leaving it in place")
			// no need to delete any parents as they are needed by this node and its
			// remaining children
			break
		}

		if len(bNode.Children) == 0 {
			corruptIndex.Inc()
			log.WithFields(log.Fields{
				"branch": branch,
				"node":   nodes[i],
			}).Error("memory-idx: branch has no children while trying to delete node, index is corrupt")
			break
		}

		if bNode.Children[0] != nodes[i] {
			corruptIndex.Inc()
			log.WithFields(log.Fields{
				"child":  nodes[i],
				"branch": branch,
			}).Error("memory-idx: child is not in children list for branch, index is corrupt")
			break
		}
		bNode.Children = nil
		if bNode.Leaf() {
			log.WithFields(log.Fields{
				"branch": branch,
			}).Error("memory-idx: branch is also a leaf node, keeping it")
			break
		}
		log.WithFields(log.Fields{
			"branch": branch,
		}).Error("memory-idx: branch has no children and is not a leaf node, deleting it")
		delete(tree.Items, branch)
	}

	return deletedDefs
}

// delete series from the index if they have not been seen since "oldest"
func (m *MemoryIdx) Prune(oldest time.Time) ([]idx.Archive, error) {
	oldestUnix := oldest.Unix()
	orgs := make(map[uint32]struct{})
	log.Info("memory-idx: pruning stale metricDefs across all orgs")
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

	m.RLock()
DEFS:
	for _, def := range m.defById {
		if atomic.LoadInt64(&def.LastUpdate) >= oldestUnix {
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
				if atomic.LoadInt64(&m.defById[id].LastUpdate) >= oldestUnix {
					continue DEFS
				}
			}

			toPruneUntagged[def.OrgId][n.Path] = struct{}{}
		} else {
			defs := m.defByTagSet.defs(def.OrgId, def.NameWithTags())
			// if any other MetricDef with the same tag set is not expired yet,
			// then we do not want to prune any of them
			for def := range defs {
				if atomic.LoadInt64(&def.LastUpdate) >= oldestUnix {
					continue DEFS
				}
			}

			for def := range defs {
				toPruneTagged[def.OrgId][def.Id] = struct{}{}
			}
		}
	}
	m.RUnlock()

	for org, ids := range toPruneTagged {
		if len(ids) == 0 {
			continue
		}
		m.Lock()
		defs := m.deleteTaggedByIdSet(org, ids)
		m.Unlock()
		pruned = append(pruned, defs...)
	}

	for org, paths := range toPruneUntagged {
		if len(paths) == 0 {
			continue
		}

		m.Lock()
		tree, ok := m.tree[org]
		if !ok {
			m.Unlock()
			continue
		}

		for path := range paths {
			n, ok := tree.Items[path]
			if !ok {
				m.Unlock()
				log.WithFields(log.Fields{
					"series": path,
					"org.id": org,
				}).Debug("memory-idx: series for orgId was identified for pruning but cannot be found")
				continue
			}

			log.WithFields(log.Fields{
				"series": n.Path,
				"org.id": org,
			}).Debug("memory-idx: series for orgId is stale, pruning it")
			defs := m.delete(org, n, true, false)
			pruned = append(pruned, defs...)
		}
		m.Unlock()
	}

	statMetricsActive.Add(-1 * len(pruned))

	log.WithFields(log.Fields{
		"time.taken": time.Since(pre).String(),
	}).Info("memory-idx: pruning stale metricDefs from memory for all orgs")

	statPruneDuration.Value(time.Since(pre))
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
				log.WithFields(log.Fields{
					"pattern": p,
					"error":   err.Error(),
				}).Debug("memory-idx: regexp failed to compile")
				return nil, errors.NewBadRequest(err.Error())
			}
			regexes = append(regexes, r)
		}

		return func(children []string) []string {
			var matches []string
			for _, r := range regexes {
				for _, c := range children {
					if r.MatchString(c) {
						log.WithFields(log.Fields{
							"pattern": r.String(),
							"child":   c,
						}).Debug("memory-idx: child matches")
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
					log.WithFields(log.Fields{
						"pattern": p,
						"child":   c,
					}).Debug("memory-0idx: child matches")
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
