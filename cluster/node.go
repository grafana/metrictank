package cluster

import (
	"encoding/json"
	"sync"
	"time"
)

//go:generate stringer -type=NodeState
type NodeState int

const (
	NodeNotReady NodeState = iota
	NodeReady
)

type Node struct {
	name    string
	version string
	started time.Time

	sync.RWMutex
	primary       bool
	primaryChange time.Time
	state         NodeState
	stateChange   time.Time
}

func (n *Node) GetName() string {
	return n.name
}

// Returns True if the node is a Primary node which
// perists data to the Backend Store (cassandra)
func (n *Node) IsPrimary() bool {
	n.RLock()
	p := n.primary
	n.RUnlock()
	return p
}

// Returns true if the node is a ready to accept requests
// from users.
func (n *Node) IsReady() bool {
	n.RLock()
	p := n.state == NodeReady
	n.RUnlock()
	return p
}

func (n *Node) SetReady() {
	n.SetState(NodeReady)
}

func (n *Node) SetReadyIn(t time.Duration) {
	go func() {
		// wait for warmupPeriod before marking ourselves
		// as ready.
		time.Sleep(t)
		n.SetReady()
	}()
}

func (n *Node) SetPrimary(p bool) {
	n.Lock()
	n.primary = p
	n.primaryChange = time.Now()
	n.Unlock()
}

func (n *Node) SetState(s NodeState) {
	n.Lock()
	n.state = s
	n.stateChange = time.Now()
	n.Unlock()
}

// provide thread safe json Marshaling
func (n *Node) MarshalJSON() ([]byte, error) {
	n.RLock()
	defer n.RUnlock()
	return json.Marshal(&nodeJS{
		Name:          n.name,
		Version:       n.version,
		Primary:       n.primary,
		PrimaryChange: n.primaryChange,
		State:         n.state.String(),
		StateChange:   n.stateChange,
	})
}

// intermediary struct to enable thread safe json marshaling
type nodeJS struct {
	Name          string    `json:"name"`
	Version       string    `json:"version"`
	Primary       bool      `json:"primary"`
	PrimaryChange time.Time `json:"primaryChange"`
	State         string    `json:"state"`
	Started       time.Time `json:"started"`
	StateChange   time.Time `json:"stateChange"`
}
