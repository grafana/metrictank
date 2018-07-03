package cluster

import (
	"context"
	"sort"
	"time"
)

type MockNode struct {
	isLocal      bool
	isReady      bool
	name         string
	postResponse []byte
	partitions   []int32
	priority     int
}

func (n *MockNode) IsLocal() bool {
	return n.isLocal
}

func (n *MockNode) IsReady() bool {
	return n.isReady
}

func (n *MockNode) GetPartitions() []int32 {
	return n.partitions
}

func (n *MockNode) GetPriority() int {
	return n.priority
}

func (n MockNode) Post(ctx context.Context, name, path string, body Traceable) ([]byte, error) {
	return n.postResponse, nil
}

func (n *MockNode) GetName() string {
	return n.name
}

func NewMockNode(isLocal bool, name string, postResponse []byte) *MockNode {
	return &MockNode{
		isLocal:      isLocal,
		name:         name,
		postResponse: postResponse,
	}
}

type MockClusterManager struct {
	Peers           []*MockNode
	membersForQuery []*MockNode
	thisNode        int
	isPrimary       bool
	isReady         bool
	partitions      []int32
}

func (c *MockClusterManager) MemberList() []Node {
	return mockToIf(c.Peers)
}

func (c *MockClusterManager) ThisNode() Node {
	return c.Peers[c.thisNode]
}

func (c *MockClusterManager) Start()                     {}
func (c *MockClusterManager) Stop()                      {}
func (c *MockClusterManager) SetPriority(prio int)       {}
func (c *MockClusterManager) SetPrimary(primary bool)    {}
func (c *MockClusterManager) SetReady()                  {}
func (c *MockClusterManager) SetReadyIn(t time.Duration) {}
func (c *MockClusterManager) SetState(NodeState)         {}

func (c *MockClusterManager) IsPrimary() bool {
	return c.isPrimary
}

func (c *MockClusterManager) IsReady() bool {
	return c.isReady
}

func (c *MockClusterManager) GetPartitions() []int32 {
	return c.partitions
}

func (c *MockClusterManager) SetPartitions(partitions []int32) {
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })

	c.partitions = partitions
}

func (c *MockClusterManager) Join(peers []string) (int, error) {
	return 0, nil
}

func InitMock() *MockClusterManager {
	manager := &MockClusterManager{}
	Manager = manager
	return manager
}

func mockToIf(in []*MockNode) []Node {
	out := make([]Node, len(in))
	for i, m := range in {
		out[i] = m
	}
	return out
}
