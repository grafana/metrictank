package memory

import log "github.com/sirupsen/logrus"

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
	out    chan *Node
	errors chan error
	done   bool // wether there's any more items to process

	// all returned responses (without 'from' filtering), which upon Close() will get added to the findcache
	found []*Node
}

// NewFindIter creates a new Find iterator. findCache can be nil to disable caching. orgID is only used for caching
func NewFindIter(tree *Tree, orgID uint32, pattern string, findCache *FindCache) *FindIter {
	f := FindIter{
		tree:      tree,
		orgID:     orgID,
		pattern:   pattern,
		findCache: findCache,
		out:       make(chan *Node),
		errors:    make(chan error),
	}
	go findWorker(f.tree, f.pattern, f.out, f.errors)
	return &f
}

// Next returns the next node (if any), and an error (if any)
// the iteration is complete when both are nil
func (f *FindIter) Next() (*Node, error) {
	select {
	case n, ok := <-f.out:
		if !ok {
			f.done = true
			return nil, nil
		}
		f.found = append(f.found, n)
		return n, nil
	case err := <-f.errors:
		f.done = true
		// handles both receiving an err and channel close
		return nil, err
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
