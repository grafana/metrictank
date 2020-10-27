package memory

import (
	"strings"

	"github.com/grafana/metrictank/errors"
	log "github.com/sirupsen/logrus"
)

type Iter interface {
	Next() (*Node, error)
	Close()
}

// NodesIterator returns nodes from a predefined slice
// it does not add data to the cache
type NodesIterator struct {
	next  int
	nodes []*Node
}

func (n *NodesIterator) Next() (*Node, error) {
	if n.next < len(n.nodes) {
		node := n.nodes[n.next]
		n.next++
		return node, nil
	}
	return nil, nil
}

func (n *NodesIterator) Close() {}

// FindIter is an iterator that provides all nodes from the index matching a certain pattern
// callers should hold the lock on the index while creating the iterator, calling Next(), and calling Close()
type FindIter struct {
	// config
	tree      *Tree
	orgID     uint32 // only used for caching
	pattern   string
	findCache *FindCache // may be nil to disable caching

	// iteration config & state
	nodes    []string        // foo.bar.z* becomes ["foo", "bar", "z*"]
	matchers []matcher       // match functions (only set for those nodes that lie in the "search space" e.g. for nodes[start:]
	start    int             // start marks the beginning of the search space. e.g. for foo.bar.z*, start=2
	pos      int             // position within nodes that we're currently working on. start <= pos <= len(nodes).
	branch   string          // the branch we're currently working on
	stack    []*stackElement // stack[0] corresponds to node[start]
	done     bool            // wether there's any more items to process

	// all returned responses (without 'from' filtering), which upon Close() will get added to the findcache
	found []*Node
}

// NewFindIter creates a new Find iterator. findCache can be nil to disable caching. orgID is only used for caching
func NewFindIter(tree *Tree, orgID uint32, pattern string, findCache *FindCache) (*FindIter, error) {
	f := FindIter{
		tree:      tree,
		orgID:     orgID,
		pattern:   pattern,
		findCache: findCache,
	}
	return &f, f.init()
}

type stackElement struct {
	node     *Node
	children []string // every time we look at our node, we don't want to re-evaluate the matchers on the node's children
	next     int      // child position that needs to be looked at next
}

func (f *FindIter) init() error {
	if strings.Index(f.pattern, ";") == -1 {
		f.nodes = strings.Split(f.pattern, ".")
	} else {
		pieces := strings.SplitN(f.pattern, ";", 2)
		f.nodes = strings.Split(pieces[0], ".")
		tags := pieces[1]
		f.nodes[len(f.nodes)-1] += ";" + tags
	}

	// start is the index of the first node with special chars, or one past the last node if exact
	// for a query like foo.bar.baz, start is 3
	// for a query like foo.bar.* or foo.bar, start is 2
	// for a query like foo.b*.baz, start is 1
	f.start = len(f.nodes)
	for i := 0; i < len(f.nodes); i++ {
		if strings.ContainsAny(f.nodes[i], "*{}[]?") {
			log.Debugf("memory-idx: found first pattern sequence at node %s pos %d", f.nodes[i], i)
			f.start = i
			break
		}
	}
	f.pos = f.start

	f.matchers = make([]matcher, 0, len(f.nodes))
	for i := f.start; i < len(f.nodes); i++ {
		matcher, err := getMatcher(f.nodes[i])

		if err != nil {
			return err
		}

		f.matchers[i] = matcher
	}

	if f.start != 0 { // f.branch = "" if f.start = 0
		f.branch = strings.Join(f.nodes[:f.start], ".")
	}

	log.Debugf("memory-idx: starting search at node %q", f.branch)
	node, ok := f.tree.Items[f.branch]

	if !ok {
		log.Debugf("memory-idx: branch %q does not exist in the index", f.branch)
		f.done = true
		return nil
	}

	if node == nil {
		corruptIndex.Inc()
		log.Errorf("memory-idx: startNode is nil. patt=%q,start=%d,branch=%q", f.pattern, f.start, f.branch)
		return errors.NewInternal("hit an empty path in the index")
	}

	// pos=len(nodes) is a special case where pattern=static string. the only output is our starting node, we don't need to find its children
	var children []string
	if f.pos < len(f.nodes) {
		if !node.HasChildren() {
			log.Debugf("memory-idx: end of branch reached at %s with no match found for %s", node.Path, f.pattern)
			f.done = true
			return nil
		}
		log.Debugf("memory-idx: searching %d children of %s that match %s", len(node.Children), node.Path, f.nodes[f.pos]) // note we know f.pos is always 0 here
		children = f.matchers[f.pos](node.Children)
		if len(children) == 0 {
			f.done = true
			return nil
		}
	}
	f.stack = append(f.stack, &stackElement{
		node:     node,
		children: children,
	})

	return nil
}

// Next returns the next node (if any), and an error (if any)
// the iteration is complete when both are nil
func (f *FindIter) Next() (*Node, error) {
	if f.done {
		return nil, nil
	}
	node, err := f.next()
	if err != nil {
		return node, err
	}
	f.found = append(f.found, node)
	return node, err
}

func (f *FindIter) lookup(parent, child string) (*Node, error) {
	branch := child
	if parent != "" {
		branch = parent + "." + child
	}
	node := f.tree.Items[branch]
	if node == nil {
		corruptIndex.Inc()
		log.Errorf("memory-idx: child is nil. patt=%q,pos=%d,p=%q,path=%q", f.pattern, f.pos, f.nodes[f.pos], f.branch)
		return nil, errors.NewInternal("hit an empty path in the index")
	}
	return node, nil
}

// next will do a depth-first-search for the next node that matches the pattern
func (f *FindIter) next() (*Node, error) {
	e := f.stack[f.pos-f.start]
	if f.pos == len(f.nodes) {
		// special case. the query was a.static.string which only resolves to a single node
		f.done = true
		return e.node, nil
	}
	if f.pos == len(f.nodes)-1 {
		// we are looking as deep down the tree as we're willing to go
		goto TryNextSibling
	}
	// we're somewhere halfway down the tree
	goto Grow

TryNextSibling:
	if e.next < len(e.children) {
		if f.pos == len(f.nodes)-1 {
			node, err := f.lookup(e.node.Path, e.children[e.next])
			e.next++
			if err != nil {
				f.done = true
			}
			return node, err
		}
		// implied that f.pos < len(e.nodes)-1
		goto Grow
	} else {
		goto Unwind
	}
Unwind:
	for {
		if f.pos == f.start {
			f.done = true
			return nil, nil
		}
		f.pos--
		f.stack = f.stack[:f.pos-f.start]
		e := f.stack[f.pos-1]
		if e.next < len(e.children) {
			// we have children to process. we know we can't be at the end of the tree, so we must grow again
			goto Grow
		}
	}
Grow:
	for {
		node, err := f.lookup(e.node.Path, e.children[e.next])
		if err != nil {
			f.done = true
			return nil, err
		}
		e.next++
		if !node.HasChildren() {
			log.Debugf("memory-idx: end of branch reached at %s with no match found for %s", node.Path, f.pattern)
			goto TryNextSibling
		}
		log.Debugf("memory-idx: searching %d children of %s that match %s", len(node.Children), node.Path, f.nodes[f.pos])
		matches := f.matchers[f.pos](node.Children)
		if len(matches) == 0 {
			goto TryNextSibling
		}
		// we have matches! grow our stack again
		f.pos++
		f.stack = append(f.stack, &stackElement{
			node:     node,
			children: matches,
		})
	}
}

// Close closes the iterator and adds the results to the findcache
// Callers may only call Close() once, after the iteration completed
// but they not need to call Close(), for example when the results should not be cached
func (f *FindIter) Close() {
	if !f.done {
		panic("Close called on !done FindIter")
	}
	log.Debugf("memory-idx: reached pattern length. %d nodes matched", len(f.found))
	if f.findCache == nil {
		return
	}
	f.findCache.Add(f.orgID, f.pattern, f.found)
}
