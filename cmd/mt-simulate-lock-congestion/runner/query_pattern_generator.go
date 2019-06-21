package runner

import (
	"strings"
	"sync/atomic"
)

type queryPatternGenerator interface {
	getPattern(string) string
}

type replaceRandomNodeWithAsterisk struct {
	callCount uint32
}

func newQueryPatternGenerator() queryPatternGenerator {
	return &replaceRandomNodeWithAsterisk{}
}

func (r *replaceRandomNodeWithAsterisk) getPattern(name string) string {
	callNumber := atomic.AddUint32(&r.callCount, 1)
	nodes := strings.Split(name, ".")
	replaceNode := int(callNumber) % len(nodes)
	nodes[replaceNode] = "*"
	nodes[replaceNode/2] = "*"
	nodes[(replaceNode*2)%len(nodes)] = "*"
	return strings.Join(nodes, ".")
}
