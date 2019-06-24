package query

import (
	"strings"
	"sync/atomic"
)

type ReplaceRandomNode struct {
	getExistingName func() string // function that returns a name that's already been added
	callCount       uint32
}

func NewNodeReplacer(getExistingName func() string) QueryGenerator {
	return &ReplaceRandomNode{
		getExistingName: getExistingName,
	}
}

func (r *ReplaceRandomNode) Start() {}

func (r *ReplaceRandomNode) GetPattern() string {
	callNumber := atomic.AddUint32(&r.callCount, 1)
	nodes := strings.Split(r.getExistingName(), ".")
	replaceNode := int(callNumber) % len(nodes)

	// MT does not support patterns where the first node is a "*", so don't query those
	if replaceNode > 0 {
		nodes[replaceNode] = "*"
	}
	if replaceNode/2 > 0 {
		nodes[replaceNode/2] = "*"
	}
	if (replaceNode*2)%len(nodes) > 0 {
		nodes[(replaceNode*2)%len(nodes)] = "*"
	}
	return strings.Join(nodes, ".")
}
