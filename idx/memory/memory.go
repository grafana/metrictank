package memory

import (
	"flag"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
	"gopkg.in/raintank/schema.v1"
)

var (
	// metric idx.memory.update is the number of updates to the memory idx
	statUpdate = stats.NewCounter32("idx.memory.ops.update")
	// metric idx.memory.add is the number of additions to the memory idx
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

	Enabled        bool
	matchCacheSize int
	tagSupport     bool
)

func ConfigSetup() {
	memoryIdx := flag.NewFlagSet("memory-idx", flag.ExitOnError)
	memoryIdx.BoolVar(&Enabled, "enabled", false, "")
	memoryIdx.BoolVar(&tagSupport, "tag-support", false, "")
	memoryIdx.IntVar(&matchCacheSize, "match-cache-size", 1000, "")
	globalconf.Register("memory-idx", memoryIdx)
}

type Tree struct {
	Items map[string]*Node // key is the full path of the node.
}

type TagIDs map[idx.MetricID]struct{} // set of ids
type TagValue map[string]TagIDs       // value -> set of ids
type TagIndex map[string]TagValue     // key -> list of values

type Node struct {
	Path     string
	Children []string
	Defs     []string
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
	DefById map[string]*idx.Archive
	Tree    map[int]*Tree

	// org id -> key name -> key value -> id
	Tags map[int]TagIndex
}

func New() *MemoryIdx {
	return &MemoryIdx{
		DefById: make(map[string]*idx.Archive),
		Tree:    make(map[int]*Tree),
		Tags:    make(map[int]TagIndex),
	}
}

func (m *MemoryIdx) Init() error {
	return nil
}

func (m *MemoryIdx) Stop() {
	return
}

func (m *MemoryIdx) AddOrUpdate(data *schema.MetricData, partition int32) idx.Archive {
	pre := time.Now()
	m.Lock()
	defer m.Unlock()
	existing, ok := m.DefById[data.Id]
	if ok {
		log.Debug("metricDef with id %s already in index.", data.Id)
		existing.LastUpdate = data.Time
		existing.Partition = partition
		statUpdate.Inc()
		statUpdateDuration.Value(time.Since(pre))
		return *existing
	}

	def := schema.MetricDefinitionFromMetricData(data)
	def.Partition = partition
	archive := m.add(def)
	statMetricsActive.Inc()
	statAddDuration.Value(time.Since(pre))

	if tagSupport {
		m.indexTags(def)
	}

	return archive
}

func (m *MemoryIdx) Update(entry idx.Archive) {
	m.Lock()
	if _, ok := m.DefById[entry.Id]; !ok {
		m.Unlock()
		return
	}
	*(m.DefById[entry.Id]) = entry
	m.Unlock()
}

// indexTags reads the tags of a given metric definition and creates the
// corresponding tag index entries to refer to it. It assumes a lock is
// already held.
func (m *MemoryIdx) indexTags(def *schema.MetricDefinition) {
	tags, ok := m.Tags[def.OrgId]
	if !ok {
		tags = make(TagIndex)
		m.Tags[def.OrgId] = tags
	}
	for _, tag := range def.Tags {
		tagSplits := strings.SplitN(tag, "=", 2)
		if len(tagSplits) < 2 {
			// should never happen because every tag in the index
			// must have a valid format
			InvalidTagInIndex.Inc()
			log.Error(3, "memory-idx: Tag %q of id %q has an invalid format", tag, def.Id)
			continue
		}

		tagName := tagSplits[0]
		tagValue := tagSplits[1]

		if _, ok = tags[tagName]; !ok {
			tags[tagName] = make(TagValue)
		}

		if _, ok = tags[tagName][tagValue]; !ok {
			tags[tagName][tagValue] = make(TagIDs)
		}

		id, err := idx.NewMetricIDFromString(def.Id)
		if err != nil {
			// should never happen because all IDs in the index must have
			// a valid format
			CorruptIndex.Inc()
			log.Error(3, "memory-idx: ID %q has invalid format", def.Id)
			continue
		}
		tags[tagName][tagValue][id] = struct{}{}
	}
}

// deindexTags takes a given metric definition and removes all references
// to it from the tag index. It assumes a lock is already held.
func (m *MemoryIdx) deindexTags(def *schema.MetricDefinition) {
	tags, ok := m.Tags[def.OrgId]
	if !ok {
		return
	}

	for _, tag := range def.Tags {
		tagSplits := strings.SplitN(tag, "=", 2)
		if len(tagSplits) < 2 {
			// should never happen because every tag in the index
			// must have a valid format
			InvalidTagInIndex.Inc()
			log.Error(3, "memory-idx: Tag %q of id %q has an invalid format", tag, def.Id)
			continue
		}

		tagName := tagSplits[0]
		tagValue := tagSplits[1]

		id, err := idx.NewMetricIDFromString(def.Id)
		if err != nil {
			// should never happen because all IDs in the index must have
			// a valid format
			CorruptIndex.Inc()
			log.Error(3, "memory-idx: ID %q has invalid format", def.Id)
			continue
		}
		delete(tags[tagName][tagValue], id)

		if len(tags[tagName][tagValue]) == 0 {
			delete(tags[tagName], tagValue)
			if len(tags[tagName]) == 0 {
				delete(tags, tagName)
			}
		}
	}
}

// Used to rebuild the index from an existing set of metricDefinitions.
func (m *MemoryIdx) Load(defs []schema.MetricDefinition) int {
	m.Lock()
	var pre time.Time
	var num int
	for i := range defs {
		def := &defs[i]
		pre = time.Now()
		if _, ok := m.DefById[def.Id]; ok {
			continue
		}
		m.add(def)

		if tagSupport {
			m.indexTags(def)
		}

		// as we are loading the metricDefs from a persistent store, set the lastSave
		// to the lastUpdate timestamp.  This wont exactly match the true lastSave Timstamp,
		// but it will be close enough and it will always be true that the lastSave was at
		// or after this time.  For metrics that are sent at or close to real time (the typical
		// use case), then the value will be within a couple of seconds of the true lastSave.
		m.DefById[def.Id].LastSave = uint32(def.LastUpdate)
		num++
		statMetricsActive.Inc()
		statAddDuration.Value(time.Since(pre))
	}
	m.Unlock()
	return num
}

func (m *MemoryIdx) add(def *schema.MetricDefinition) idx.Archive {
	path := def.Name
	schemaId, _ := mdata.MatchSchema(def.Name, def.Interval)
	aggId, _ := mdata.MatchAgg(def.Name)
	archive := &idx.Archive{
		MetricDefinition: *def,
		SchemaId:         schemaId,
		AggId:            aggId,
	}

	//first check to see if a tree has been created for this OrgId
	tree, ok := m.Tree[def.OrgId]
	if !ok || len(tree.Items) == 0 {
		log.Debug("memory-idx: first metricDef seen for orgId %d", def.OrgId)
		root := &Node{
			Path:     "",
			Children: make([]string, 0),
			Defs:     make([]string, 0),
		}
		m.Tree[def.OrgId] = &Tree{
			Items: map[string]*Node{"": root},
		}
		tree = m.Tree[def.OrgId]
	} else {
		// now see if there is an existing branch or leaf with the same path.
		// An existing leaf is possible if there are multiple metricDefs for the same path due
		// to different tags or interval
		if node, ok := tree.Items[path]; ok {
			log.Debug("memory-idx: existing index entry for %s. Adding %s to Defs list", path, def.Id)
			node.Defs = append(node.Defs, def.Id)
			m.DefById[def.Id] = archive
			statAdd.Inc()
			return *archive
		}
	}

	// now walk backwards through the node path to find the first branch which exists that
	// this path extends.
	pos := strings.LastIndex(path, ".")
	prevPos := len(path)
	for pos != -1 {
		branch := path[:pos]
		prevNode := path[pos+1 : prevPos]
		if n, ok := tree.Items[branch]; ok {
			log.Debug("memory-idx: adding %s as child of %s", prevNode, n.Path)
			n.Children = append(n.Children, prevNode)
			break
		}

		log.Debug("memory-idx: creating branch %s with child %s", branch, prevNode)
		tree.Items[branch] = &Node{
			Path:     branch,
			Children: []string{prevNode},
			Defs:     make([]string, 0),
		}

		prevPos = pos
		pos = strings.LastIndex(branch, ".")
	}

	if pos == -1 {
		// need to add to the root node.
		branch := path[:prevPos]
		log.Debug("memory-idx: no existing branches found for %s.  Adding to the root node.", branch)
		n := tree.Items[""]
		n.Children = append(n.Children, branch)
	}

	// Add leaf node
	log.Debug("memory-idx: creating leaf %s", path)
	tree.Items[path] = &Node{
		Path:     path,
		Children: []string{},
		Defs:     []string{def.Id},
	}
	m.DefById[def.Id] = archive
	statAdd.Inc()

	return *archive
}

func (m *MemoryIdx) Get(id string) (idx.Archive, bool) {
	pre := time.Now()
	m.RLock()
	defer m.RUnlock()
	def, ok := m.DefById[id]
	statGetDuration.Value(time.Since(pre))
	if ok {
		return *def, ok
	}
	return idx.Archive{}, ok
}

// GetPath returns the node under the given org and path.
// this is an alternative to Find for when you have a path, not a pattern, and want to lookup in a specific org tree only.
func (m *MemoryIdx) GetPath(orgId int, path string) []idx.Archive {
	m.RLock()
	defer m.RUnlock()
	tree, ok := m.Tree[orgId]
	if !ok {
		return nil
	}
	node := tree.Items[path]
	if node == nil {
		return nil
	}
	archives := make([]idx.Archive, len(node.Defs))
	for i, def := range node.Defs {
		archive := m.DefById[def]
		archives[i] = *archive
	}
	return archives
}

func (m *MemoryIdx) Tag(orgId int, tag string, from int64) map[string]uint32 {
	if !tagSupport {
		log.Warn("memory-idx: received tag query, but tag support is disabled")
		return nil
	}

	m.RLock()
	defer m.RUnlock()

	var tags TagIndex
	var ok bool
	if tags, ok = m.Tags[orgId]; !ok {
		return nil
	}

	result := make(map[string]uint32)

	for value, ids := range tags[tag] {
		valueCnt := uint32(0)
		for id := range ids {
			var def *idx.Archive
			var ok bool
			if def, ok = m.DefById[id.String()]; !ok {
				// should never happen because every ID that is in the tag index
				// must be present in the byId lookup table
				CorruptIndex.Inc()
				log.Error(3, "memory-idx: ID %q is in tag index but not in the byId lookup table", id.String())
				continue
			}

			if def.LastUpdate < from {
				continue
			}

			valueCnt++
		}
		if valueCnt > 0 {
			result[value] = valueCnt
		}
	}

	return result
}

func (m *MemoryIdx) TagList(orgId int) []string {
	if !tagSupport {
		log.Warn("memory-idx: received tag query, but tag support is disabled")
		return nil
	}

	m.RLock()
	defer m.RUnlock()

	var tags TagIndex
	var ok bool
	if tags, ok = m.Tags[orgId]; !ok {
		return nil
	}

	results := make([]string, 0, len(tags))

	for k := range tags {
		results = append(results, k)
	}

	return results
}

func (m *MemoryIdx) FindByTag(orgId int, expressions []string, from int64) (map[idx.MetricID]struct{}, error) {
	query, err := NewTagQuery(expressions, from)
	if err != nil {
		return nil, err
	}

	seriesMap := m.IdsByTagQuery(orgId, query)
	return seriesMap, nil
}

func (m *MemoryIdx) IdsByTagQuery(orgId int, query TagQuery) TagIDs {
	m.RLock()
	defer m.RUnlock()

	tree, ok := m.Tags[orgId]
	if !ok {
		return nil
	}

	res, err := query.Run(tree, m.DefById)
	if err != nil {
		return nil
	}

	return res
}

func (m *MemoryIdx) Find(orgId int, pattern string, from int64) ([]idx.Node, error) {
	pre := time.Now()
	m.RLock()
	defer m.RUnlock()
	matchedNodes, err := m.find(orgId, pattern)
	if err != nil {
		return nil, err
	}
	publicNodes, err := m.find(-1, pattern)
	if err != nil {
		return nil, err
	}
	matchedNodes = append(matchedNodes, publicNodes...)
	log.Debug("memory-idx: %d nodes matching pattern %s found", len(matchedNodes), pattern)
	results := make([]idx.Node, 0)
	seen := make(map[string]struct{})
	// if there are public (orgId -1) and private leaf nodes with the same series
	// path, then the public metricDefs will be excluded.
	for _, n := range matchedNodes {
		if _, ok := seen[n.Path]; !ok {
			idxNode := idx.Node{
				Path:        n.Path,
				Leaf:        n.Leaf(),
				HasChildren: n.HasChildren(),
			}
			if idxNode.Leaf {
				idxNode.Defs = make([]idx.Archive, 0, len(n.Defs))
				for _, id := range n.Defs {
					def := m.DefById[id]
					if from != 0 && def.LastUpdate < from {
						statFiltered.Inc()
						log.Debug("memory-idx: from is %d, so skipping %s which has LastUpdate %d", from, def.Id, def.LastUpdate)
						continue
					}
					log.Debug("memory-idx Find: adding to path %s archive id=%s name=%s int=%d schemaId=%d aggId=%d lastSave=%d", n.Path, def.Id, def.Name, def.Interval, def.SchemaId, def.AggId, def.LastSave)
					idxNode.Defs = append(idxNode.Defs, *def)
				}
				if len(idxNode.Defs) == 0 {
					continue
				}
			}
			results = append(results, idxNode)
			seen[n.Path] = struct{}{}
		} else {
			log.Debug("memory-idx: path %s already seen", n.Path)
		}
	}
	log.Debug("memory-idx: %d nodes has %d unique paths.", len(matchedNodes), len(results))
	statFindDuration.Value(time.Since(pre))
	return results, nil
}

func (m *MemoryIdx) find(orgId int, pattern string) ([]*Node, error) {
	var results []*Node
	tree, ok := m.Tree[orgId]
	if !ok {
		log.Debug("memory-idx: orgId %d has no metrics indexed.", orgId)
		return results, nil
	}

	nodes := strings.Split(pattern, ".")

	// pos is the index of the first node with special chars, or one past the last node if exact
	// for a query like foo.bar.baz, pos is 3
	// for a query like foo.bar.* or foo.bar, pos is 2
	// for a query like foo.b*.baz, pos is 1
	pos := len(nodes)
	for i := 0; i < len(nodes); i++ {
		if strings.ContainsAny(nodes[i], "*{}[]?") {
			log.Debug("memory-idx: found first pattern sequence at node %s pos %d", nodes[i], i)
			pos = i
			break
		}
	}
	var startNode *Node
	if pos == 0 {
		//we need to start at the root.
		log.Debug("memory-idx: starting search at the root node")
		startNode = tree.Items[""]
	} else {
		branch := strings.Join(nodes[0:pos], ".")
		log.Debug("memory-idx: starting search at branch %s", branch)
		startNode, ok = tree.Items[branch]
		if !ok {
			log.Debug("memory-idx: branch %s does not exist in the index for orgId %d", branch, orgId)
			return results, nil
		}
	}

	children := []*Node{startNode}
	for i := pos; i < len(nodes); i++ {
		p := nodes[i]

		matcher, err := getMatcher(p)

		if err != nil {
			return nil, err
		}

		grandChildren := make([]*Node, 0)
		for _, c := range children {
			if !c.HasChildren() {
				log.Debug("memory-idx: end of branch reached at %s with no match found for %s", c.Path, pattern)
				// expecting a branch
				continue
			}
			log.Debug("memory-idx: searching %d children of %s that match %s", len(c.Children), c.Path, nodes[i])
			matches := matcher(c.Children)
			for _, m := range matches {
				newBranch := c.Path + "." + m
				if c.Path == "" {
					newBranch = m
				}
				grandChildren = append(grandChildren, tree.Items[newBranch])
			}
		}
		children = grandChildren
		if len(children) == 0 {
			log.Debug("memory-idx: pattern does not match any series.")
			break
		}
	}

	log.Debug("memory-idx: reached pattern length. %d nodes matched", len(children))
	for _, c := range children {
		results = append(results, c)
	}

	return results, nil
}

func (m *MemoryIdx) List(orgId int) []idx.Archive {
	pre := time.Now()
	m.RLock()
	defer m.RUnlock()
	orgs := []int{-1, orgId}
	if orgId == -1 {
		log.Info("memory-idx: returning all metricDefs for all orgs")
		orgs = make([]int, len(m.Tree))
		i := 0
		for org := range m.Tree {
			orgs[i] = org
			i++
		}
	}
	defs := make([]idx.Archive, 0)
	for _, org := range orgs {
		tree, ok := m.Tree[org]
		if !ok {
			continue
		}
		for _, n := range tree.Items {
			if !n.Leaf() {
				continue
			}
			for _, id := range n.Defs {
				defs = append(defs, *m.DefById[id])
			}
		}
	}
	statListDuration.Value(time.Since(pre))

	return defs
}

func (m *MemoryIdx) Delete(orgId int, pattern string) ([]idx.Archive, error) {
	var deletedDefs []idx.Archive
	pre := time.Now()
	m.Lock()
	defer m.Unlock()
	found, err := m.find(orgId, pattern)
	if err != nil {
		return nil, err
	}

	for _, f := range found {
		deleted := m.delete(orgId, f, true)
		statMetricsActive.DecUint32(uint32(len(deleted)))
		deletedDefs = append(deletedDefs, deleted...)
	}
	statDeleteDuration.Value(time.Since(pre))

	return deletedDefs, nil
}

func (m *MemoryIdx) delete(orgId int, n *Node, deleteEmptyParents bool) []idx.Archive {
	tree := m.Tree[orgId]
	deletedDefs := make([]idx.Archive, 0)
	if n.HasChildren() {
		log.Debug("memory-idx: deleting branch %s", n.Path)
		// walk up the tree to find all leaf nodes and delete them.
		for _, child := range n.Children {
			node, ok := tree.Items[n.Path+"."+child]
			if !ok {
				CorruptIndex.Inc()
				log.Error(3, "memory-idx: node %s missing. Index is corrupt.", n.Path+"."+child)
				continue
			}
			log.Debug("memory-idx: deleting child %s from branch %s", node.Path, n.Path)
			deleted := m.delete(orgId, node, false)
			deletedDefs = append(deletedDefs, deleted...)
		}
	}

	// delete the metricDefs
	for _, id := range n.Defs {
		log.Debug("memory-idx: deleting %s from index", id)
		deletedDefs = append(deletedDefs, *m.DefById[id])
		delete(m.DefById, id)
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
		branch := strings.Join(nodes[0:i], ".")
		log.Debug("memory-idx: removing %s from branch %s", nodes[i], branch)
		bNode, ok := tree.Items[branch]
		if !ok {
			CorruptIndex.Inc()
			log.Error(3, "memory-idx: node %s missing. Index is corrupt.", branch)
			continue
		}
		if len(bNode.Children) > 1 {
			newChildren := make([]string, 0, len(bNode.Children)-1)
			for _, child := range bNode.Children {
				if child != nodes[i] {
					newChildren = append(newChildren, child)
				} else {
					log.Debug("memory-idx: %s removed from children list of branch %s", child, bNode.Path)
				}
			}
			bNode.Children = newChildren
			log.Debug("memory-idx: branch %s has other children. Leaving it in place", bNode.Path)
			// no need to delete any parents as they are needed by this node and its
			// remaining children
			break
		}

		if len(bNode.Children) == 0 {
			CorruptIndex.Inc()
			log.Error(3, "memory-idx: branch %s has no children while trying to delete %s. Index is corrupt", branch, nodes[i])
			break
		}

		if bNode.Children[0] != nodes[i] {
			CorruptIndex.Inc()
			log.Error(3, "memory-idx: %s not in children list for branch %s. Index is corrupt", nodes[i], branch)
			break
		}
		bNode.Children = make([]string, 0)
		if bNode.Leaf() {
			log.Debug("memory-idx: branch %s is also a leaf node, keeping it.", branch)
			break
		}
		log.Debug("memory-idx: branch %s has no children and is not a leaf node, deleting it.", branch)
		delete(tree.Items, branch)
	}

	if tagSupport {
		for _, def := range deletedDefs {
			m.deindexTags(&(def.MetricDefinition))
		}
	}

	return deletedDefs
}

// delete series from the index if they have not been seen since "oldest"
func (m *MemoryIdx) Prune(orgId int, oldest time.Time) ([]idx.Archive, error) {
	oldestUnix := oldest.Unix()
	var pruned []idx.Archive
	pre := time.Now()
	m.RLock()
	orgs := []int{orgId}
	if orgId == -1 {
		log.Info("memory-idx: pruning stale metricDefs across all orgs")
		orgs = make([]int, len(m.Tree))
		i := 0
		for org := range m.Tree {
			orgs[i] = org
			i++
		}
	}
	m.RUnlock()
	for _, org := range orgs {
		m.Lock()
		tree, ok := m.Tree[org]
		if !ok {
			m.Unlock()
			continue
		}

		for _, n := range tree.Items {
			if !n.Leaf() {
				continue
			}
			staleCount := 0
			for _, id := range n.Defs {
				if m.DefById[id].LastUpdate < oldestUnix {
					staleCount++
				}
			}
			if staleCount == len(n.Defs) {
				log.Debug("memory-idx: series %s for orgId:%d is stale. pruning it.", n.Path, org)
				//we need to delete this node.
				defs := m.delete(org, n, true)
				statMetricsActive.Dec()
				pruned = append(pruned, defs...)
			}
		}
		m.Unlock()
	}
	if orgId == -1 {
		log.Info("memory-idx: pruning stale metricDefs from memory for all orgs took %s", time.Since(pre).String())
	}
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
				log.Debug("memory-idx: regexp failed to compile. %s - %s", p, err)
				return nil, err
			}
			regexes = append(regexes, r)
		}

		return func(children []string) []string {
			var matches []string
			for _, r := range regexes {
				for _, c := range children {
					if r.MatchString(c) {
						log.Debug("memory-idx: %s =~ %s", c, r.String())
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
					log.Debug("memory-idx: %s matches %s", c, p)
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
