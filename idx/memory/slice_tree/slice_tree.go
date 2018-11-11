package memory

import (
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/raintank/schema"
)

type edges []node
type leaf struct {
	defs []schema.MKey
}

func (n *node) insertGet(label string) *node {
	if len(n.edges) == 0 {
		n.edges = append(n.edges, node{label: label})
		return &n.edges[0]
	}
	index := sort.Search(len(n.edges), func(i int) bool { return n.edges[i].label >= label })
	if index < len(n.edges) && n.edges[index].label == label {
		return &n.edges[index]
	}
	n.edges = append(n.edges, node{})
	copy(n.edges[index+1:], n.edges[index:])
	n.edges[index] = node{label: label}
	return &n.edges[index]
}

func (n *node) get(label string) *node {
	if len(n.edges) == 0 {
		return nil
	}
	index := sort.Search(len(n.edges), func(i int) bool { return n.edges[i].label >= label })
	if index < len(n.edges) && n.edges[index].label == label {
		return &n.edges[index]
	}
	return nil
}

type node struct {
	leaf  *leaf
	edges edges
	label string
}

type sliceTree struct {
	root node
}

func newSliceTree() sliceTree {
	return sliceTree{}
}

func (t *sliceTree) insert(path []string, mKey schema.MKey) {
	currentNode := &t.root
	for i, pathElement := range path {
		if currentNode.edges == nil {
			currentNode.edges = make(edges, 0)
		}
		if i == len(path)-1 {
			n := currentNode.insertGet(pathElement)
			if n.leaf != nil {
				n.leaf.defs = append(n.leaf.defs, mKey)
			} else {
				n.leaf = &leaf{defs: []schema.MKey{mKey}}
			}
		} else {
			currentNode = currentNode.insertGet(pathElement)
		}
	}
}

func (t *sliceTree) walker() *sliceTreeWalker {
	return &sliceTreeWalker{
		root:    &t.root,
		results: make(chan *leaf, runtime.GOMAXPROCS(0)),
	}
}

type sliceTreeWalker struct {
	wg      sync.WaitGroup
	results chan *leaf
	root    *node
}

func (t *sliceTreeWalker) reset() {
	t.results = make(chan *leaf, runtime.GOMAXPROCS(0))
}

func (t *sliceTreeWalker) get(path []string) []schema.MKey {
	t.wg.Add(1)
	go t.getBranch(t.root, path, true)

	go func() {
		t.wg.Wait()
		close(t.results)
	}()

	var results []schema.MKey
	for leaf := range t.results {
		if leaf == nil {
			continue
		}
		for _, leaf := range leaf.defs {
			results = append(results, leaf)
		}
	}

	return results
}

func (t *sliceTreeWalker) getBranch(n *node, path []string, routine bool) {
	// if this getBranch call is running in a separate routine it needs to
	// notify the waitgroup once it's done
	if routine {
		defer t.wg.Done()
	}

	if len(path) == 0 {
		if n.leaf != nil {
			t.results <- n.leaf
		}
		return
	}

	if strings.ContainsAny(path[0], "*{}[]?") {
		matcher, _ := getMatcher(path[0])
		var firstMatch *node

		for _, child := range n.edges {
			if matcher(child.label) {
				// need to copy that because the next iteration will overwrite child
				childCopy := child
				if firstMatch == nil {
					firstMatch = &childCopy
					continue
				}

				t.wg.Add(1)
				go t.getBranch(&childCopy, path[1:], true)
			}
		}

		if firstMatch != nil {
			t.getBranch(firstMatch, path[1:], false)
		}
	} else {
		child := n.get(path[0])
		if child != nil {
			t.getBranch(child, path[1:], false)
		}
	}
}
