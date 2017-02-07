package memory

import (
	"flag"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
	"gopkg.in/raintank/schema.v1"
)

var (
	// metric idx.memory.update.ok is the number of successful updates to the memory idx
	statUpdateOk = stats.NewCounter32("idx.memory.update.ok")
	// metric idx.memory.add.ok is the number of successful additions to the memory idx
	statAddOk = stats.NewCounter32("idx.memory.add.ok")
	// metric idx.memory.add.fail is the number of failed additions to the memory idx
	statAddFail = stats.NewCounter32("idx.memory.add.fail")
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

	// metric idx.metrics_active is the number of currently known metrics in the index
	statMetricsActive = stats.NewGauge32("idx.metrics_active")

	Enabled bool
)

func ConfigSetup() {
	memoryIdx := flag.NewFlagSet("memory-idx", flag.ExitOnError)
	memoryIdx.BoolVar(&Enabled, "enabled", false, "")
	globalconf.Register("memory-idx", memoryIdx)
}

type Tree struct {
	Items map[string]*Node // key is the full path of the node.
}

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
	FailedAdds map[string]error // by metric id
	DefById    map[string]*schema.MetricDefinition
	Tree       map[int]*Tree
}

func New() *MemoryIdx {
	return &MemoryIdx{
		FailedAdds: make(map[string]error),
		DefById:    make(map[string]*schema.MetricDefinition),
		Tree:       make(map[int]*Tree),
	}
}

func (m *MemoryIdx) Init() error {
	return nil
}

func (m *MemoryIdx) Stop() {
	return
}

func (m *MemoryIdx) AddOrUpdate(data *schema.MetricData, partition int32) error {
	pre := time.Now()
	m.Lock()
	defer m.Unlock()
	err, ok := m.FailedAdds[data.Id]
	if ok {
		// if it failed before, it would fail again.
		// there's not much point in doing the work of trying over
		// and over again, and flooding the logs with the same failure.
		// so just trigger the stats metric as if we tried again
		statAddFail.Inc()
		return err
	}
	existing, ok := m.DefById[data.Id]
	if ok {
		log.Debug("metricDef with id %s already in index.", data.Id)
		existing.LastUpdate = data.Time
		statUpdateOk.Inc()
		statUpdateDuration.Value(time.Since(pre))
		return nil
	}

	def := schema.MetricDefinitionFromMetricData(data)
	err = m.add(def)
	if err == nil {
		statMetricsActive.Inc()
	}
	statAddDuration.Value(time.Since(pre))
	return err
}

// Used to rebuild the index from an existing set of metricDefinitions.
func (m *MemoryIdx) Load(defs []schema.MetricDefinition) (int, error) {
	m.Lock()
	var pre time.Time
	var num int
	var firstErr error
	for i := range defs {
		def := &defs[i]
		pre = time.Now()
		if _, ok := m.DefById[def.Id]; ok {
			continue
		}
		err := m.add(def)
		if err == nil {
			num++
			statMetricsActive.Inc()
		} else if firstErr == nil {
			firstErr = err
		}
		statAddDuration.Value(time.Since(pre))
	}
	m.Unlock()
	return num, firstErr
}

func (m *MemoryIdx) AddOrUpdateDef(def *schema.MetricDefinition) error {
	pre := time.Now()
	m.Lock()
	defer m.Unlock()
	if _, ok := m.DefById[def.Id]; ok {
		log.Debug("memory-idx: metricDef with id %s already in index.", def.Id)
		m.DefById[def.Id] = def
		statUpdateOk.Inc()
		statUpdateDuration.Value(time.Since(pre))
		return nil
	}
	err := m.add(def)
	if err == nil {
		statMetricsActive.Inc()
	}
	statAddDuration.Value(time.Since(pre))
	return err
}

func (m *MemoryIdx) add(def *schema.MetricDefinition) error {
	path := def.Name
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
			m.DefById[def.Id] = def
			statAddOk.Inc()
			return nil
		}
	}
	// now walk backwards through the node path to find the first branch which exists that
	// this path extends.
	nodes := strings.Split(path, ".")

	// if we're trying to insert foo.bar.baz.quux then we see if we can insert it under (in this order):
	// - foo.bar.baz (if found, startPos is 3)
	// - foo.bar (if found, startPos is 2)
	// - foo (if found, startPos is 1)
	nodePos := 0 // the index of the first word that is not part of the prefix
	var startNode *Node

	for i := len(nodes) - 1; i > 0; i-- {
		branch := strings.Join(nodes[0:i], ".")
		if n, ok := tree.Items[branch]; ok {
			log.Debug("memory-idx: Found branch %s which metricDef %s is a descendant of", branch, path)
			startNode = n
			nodePos = i
			break
		}
	}

	if nodePos == 0 && startNode == nil {
		// need to add to the root node.
		log.Debug("memory-idx: no existing branches found for %s.  Adding to the root node.", path)
		startNode = tree.Items[""]
	}

	log.Debug("memory-idx: adding %s as child of %s", nodes[nodePos], startNode.Path)
	startNode.Children = append(startNode.Children, nodes[nodePos])
	nodePos++

	// Add missing branch nodes
	for ; nodePos < len(nodes); nodePos++ {
		branch := strings.Join(nodes[0:nodePos], ".")
		log.Debug("memory-idx: creating branch %s with child %s", branch, nodes[nodePos])
		tree.Items[branch] = &Node{
			Path:     branch,
			Children: []string{nodes[nodePos]},
			Defs:     make([]string, 0),
		}
	}

	// Add leaf node
	log.Debug("memory-idx: creating leaf %s", path)
	tree.Items[path] = &Node{
		Path:     path,
		Children: []string{},
		Defs:     []string{def.Id},
	}
	m.DefById[def.Id] = def
	statAddOk.Inc()
	return nil
}

func (m *MemoryIdx) Get(id string) (schema.MetricDefinition, bool) {
	pre := time.Now()
	m.RLock()
	defer m.RUnlock()
	def, ok := m.DefById[id]
	statGetDuration.Value(time.Since(pre))
	if ok {
		return *def, ok
	}
	return schema.MetricDefinition{}, ok
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
				idxNode.Defs = make([]schema.MetricDefinition, 0, len(n.Defs))
				for _, id := range n.Defs {
					def := m.DefById[id]
					if from != 0 && def.LastUpdate < from {
						log.Debug("memory-idx: from is %d, so skipping %s which has LastUpdate %d", from, def.Id, def.LastUpdate)
						continue
					}
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

	// pos is the index of the last node we know for sure
	// for a query like foo.bar.baz, pos is 2
	// for a query like foo.bar.* or foo.bar, pos is 1
	// for a query like foo.b*.baz, pos is 0
	pos := len(nodes) - 1
	for i := 0; i < len(nodes); i++ {
		if strings.ContainsAny(nodes[i], "*{}[]?") {
			log.Debug("memory-idx: found first pattern sequence at node %s pos %d", nodes[i], i)
			pos = i - 1
			break
		}
	}
	var startNode *Node
	if pos == -1 {
		//we need to start at the root.
		log.Debug("memory-idx: starting search at the root node")
		startNode = tree.Items[""]
	} else {
		branch := strings.Join(nodes[0:pos+1], ".")
		log.Debug("memory-idx: starting search at branch %s", branch)
		startNode, ok = tree.Items[branch]
		if !ok {
			log.Debug("memory-idx: branch %s does not exist in the index for orgId %d", branch, orgId)
			return results, nil
		}
	}

	if pos == len(nodes)-1 {
		// startNode is the leaf we want.
		log.Debug("memory-idx: pattern %s was a specific branch/leaf name.", pattern)
		results = append(results, startNode)
		return results, nil
	}

	children := []*Node{startNode}
	for pos < len(nodes) {
		pos++
		if pos == len(nodes) {
			log.Debug("memory-idx: reached pattern length at node pos %d. %d nodes matched", pos, len(children))
			for _, c := range children {
				results = append(results, c)
			}
			continue
		}
		grandChildren := make([]*Node, 0)
		for _, c := range children {
			if !c.HasChildren() {
				log.Debug("memory-idx: end of branch reached at %s with no match found for %s", c.Path, pattern)
				// expecting a branch
				continue
			}
			log.Debug("memory-idx: searching %d children of %s that match %s", len(c.Children), c.Path, nodes[pos])
			matches, err := match(nodes[pos], c.Children)
			if err != nil {
				return results, err
			}
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

	return results, nil
}

func match(pattern string, candidates []string) ([]string, error) {
	var patterns []string
	if strings.ContainsAny(pattern, "{}") {
		patterns = expandQueries(pattern)
	} else {
		patterns = []string{pattern}
	}

	results := make([]string, 0)
	for _, p := range patterns {
		if strings.ContainsAny(p, "*[]?") {
			p = strings.Replace(p, "*", ".*", -1)
			p = strings.Replace(p, "?", ".?", -1)
			p = "^" + p + "$"
			r, err := regexp.Compile(p)
			if err != nil {
				log.Debug("memory-idx: regexp failed to compile. %s - %s", p, err)
				return nil, err
			}
			for _, c := range candidates {
				if r.MatchString(c) {
					log.Debug("memory-idx: %s matches %s", c, p)
					results = append(results, c)
				}
			}
		} else {
			for _, c := range candidates {
				if c == p {
					log.Debug("memory-idx: %s is exact match", c)
					results = append(results, c)
				}
			}
		}
	}
	return results, nil
}

func (m *MemoryIdx) List(orgId int) []schema.MetricDefinition {
	pre := time.Now()
	m.RLock()
	defer m.RUnlock()
	orgs := []int{-1, orgId}
	if orgId == -1 {
		log.Info("memory-idx: returing all metricDefs for all orgs")
		orgs = make([]int, len(m.Tree))
		i := 0
		for org := range m.Tree {
			orgs[i] = org
			i++
		}
	}
	defs := make([]schema.MetricDefinition, 0)
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

func (m *MemoryIdx) Delete(orgId int, pattern string) ([]schema.MetricDefinition, error) {
	var deletedDefs []schema.MetricDefinition
	pre := time.Now()
	m.Lock()
	defer m.Unlock()
	found, err := m.find(orgId, pattern)
	if err != nil {
		return nil, err
	}

	// by deleting one or more nodes in the tree, any defs that previously failed may now
	// be able to be added. An easy way to support this is just reset this map and give them
	// all a chance again
	m.FailedAdds = make(map[string]error)

	for _, f := range found {
		deleted := m.delete(orgId, f)
		statMetricsActive.Dec()
		deletedDefs = append(deletedDefs, deleted...)
	}
	statDeleteDuration.Value(time.Since(pre))
	return deletedDefs, nil
}

func (m *MemoryIdx) delete(orgId int, n *Node) []schema.MetricDefinition {
	tree := m.Tree[orgId]
	if n.HasChildren() {
		log.Debug("memory-idx: deleting branch %s", n.Path)
		// walk up the tree to find all leaf nodes and delete them.
		deletedDefs := make([]schema.MetricDefinition, 0)
		for _, child := range n.Children {
			node, ok := tree.Items[n.Path+"."+child]
			if !ok {
				log.Error(3, "memory-idx: node %s missing. Index is corrupt.", n.Path+"."+child)
				continue
			}
			log.Debug("memory-idx: deleting child %s from branch %s", node.Path, n.Path)
			deleted := m.delete(orgId, node)
			deletedDefs = append(deletedDefs, deleted...)
		}
		return deletedDefs
	}
	deletedDefs := make([]schema.MetricDefinition, len(n.Defs))
	// delete the metricDefs
	for i, id := range n.Defs {
		log.Debug("memory-idx: deleting %s from index", id)
		deletedDefs[i] = *m.DefById[id]
		delete(m.DefById, id)
	}

	// delete the leaf.
	delete(tree.Items, n.Path)

	// delete from the branches
	nodes := strings.Split(n.Path, ".")
	for i := len(nodes) - 1; i >= 0; i-- {
		branch := strings.Join(nodes[0:i], ".")
		log.Debug("memory-idx: removing %s from branch %s", nodes[i], branch)
		bNode, ok := tree.Items[branch]
		if !ok {
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

		if bNode.Children[0] != nodes[i] {
			log.Error(3, "memory-idx: %s not in children list for branch %s. Index is corrupt", nodes[i], branch)
			break
		}
		if !bNode.Leaf() {
			log.Debug("memory-idx: branch %s has no children and is not a leaf node, deleting it.", branch)
			delete(tree.Items, branch)
		}
	}

	return deletedDefs
}

// delete series from the index if they have not been seen since "oldest"
func (m *MemoryIdx) Prune(orgId int, oldest time.Time) ([]schema.MetricDefinition, error) {
	oldestUnix := oldest.Unix()
	pruned := make([]schema.MetricDefinition, 0)
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
				defs := m.delete(org, n)
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

// filepath.Match doesn't support {} because that's not posix, it's a bashism
// the easiest way of implementing this extra feature is just expanding single queries
// that contain these queries into multiple queries, who will be checked separately
// and whose results will be ORed.
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
