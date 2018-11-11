package memory

import (
	"runtime"
	"strings"
	"sync"

	"github.com/raintank/schema"
)

type edges map[string]node
type leaf struct {
	defs []schema.MKey
}

type node struct {
	leaf  *leaf
	edges edges
}

type mapTree struct {
	root node
}

func newMapTree() mapTree {
	return mapTree{root: node{edges: make(edges)}}
}

func (t *mapTree) insert(path []string, mKey schema.MKey) {
	currentNode := &t.root
	for i, pathElement := range path {
		if i == len(path)-1 {
			if n, ok := currentNode.edges[pathElement]; ok {
				if n.leaf != nil {
					n.leaf.defs = append(n.leaf.defs, mKey)
				} else {
					n.leaf = &leaf{defs: []schema.MKey{mKey}}
				}
			} else {
				currentNode.edges[pathElement] = node{
					edges: make(edges),
					leaf:  &leaf{defs: []schema.MKey{mKey}},
				}
			}
		} else {
			if childNode, ok := currentNode.edges[pathElement]; ok {
				currentNode = &childNode
			} else {
				n := &node{edges: make(edges)}
				currentNode.edges[pathElement] = *n
				currentNode = n
			}
		}
	}
}

func (t *mapTree) walker() *mapTreeWalker {
	return &mapTreeWalker{
		root:    &t.root,
		results: make(chan *leaf, runtime.GOMAXPROCS(0)),
	}
}

type mapTreeWalker struct {
	wg      sync.WaitGroup
	results chan *leaf
	root    *node
}

func (t *mapTreeWalker) reset() {
	t.results = make(chan *leaf, runtime.GOMAXPROCS(0))
}

func (t *mapTreeWalker) get(path []string) []schema.MKey {
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

func (t *mapTreeWalker) getBranch(n *node, path []string, routine bool) {
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

		for edge, child := range n.edges {
			if matcher(edge) {
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
		if child, ok := n.edges[path[0]]; ok {
			t.getBranch(&child, path[1:], false)
		}
	}
}
