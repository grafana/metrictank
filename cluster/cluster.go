package cluster

import (
	"net/url"
	"sync"
	"time"
)

var ThisCluster *Manager

func InitManager(name, version string, primary bool, started time.Time) {
	ThisCluster = NewManger(name, version, primary, started)
}

type Manager struct {
	Peers    []*Node
	Self     *Node
	shutdown chan struct{}
	sync.Mutex
}

func NewManger(name, version string, primary bool, started time.Time) *Manager {
	self := &Node{
		Name:    name,
		Primary: primary,
		Version: version,
		State:   NodeNotReady,
		Started: started,
	}
	return &Manager{
		Peers:    make([]*Node, 0),
		Self:     self,
		shutdown: make(chan struct{}),
	}
}

func (m *Manager) AddPeer(remoteAddr *url.URL) {
	m.Lock()
	m.Peers = append(m.Peers, &Node{
		RemoteAddr: remoteAddr,
		State:      NodeNotReady,
	})
	m.Unlock()
}

func (m *Manager) Run() {
	go m.poll()
}

func (m *Manager) Stop() {
	close(m.shutdown)
}

// run in separate goroutine
func (m *Manager) poll() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-m.shutdown:
			ticker.Stop()
			return
		case <-ticker.C:
			m.Lock()
			for _, node := range m.Peers {
				go node.Poll()
			}
			m.Unlock()
		}
	}
}

func (m *Manager) SetReady() {
	m.Self.SetState(NodeReady)
}

func (m *Manager) IsReady() bool {
	return m.Self.IsReady()
}

func (m *Manager) IsPrimary() bool {
	return m.Self.IsPrimary()
}

func (m *Manager) SetPrimary(p bool) {
	m.Self.SetPrimary(p)
}

func (m *Manager) SetPartitions(partitions []int32) {
	m.Self.SetPartitions(partitions)
}

// return the list of peers to broadcast requests to
// Only 1 peer per partition is returned
func (m *Manager) PeersForQuery() []*Node {
	peersMap := make(map[int32][]*Node)
	for _, part := range m.Self.Partitions {
		peersMap[part] = []*Node{m.Self}
	}
	m.Lock()
	for _, peer := range m.Peers {
		if !peer.IsReady() {
			continue
		}
		for _, part := range peer.GetPartitions() {
			l, ok := peersMap[part]
			if !ok {
				l = make([]*Node, 0)
				peersMap[part] = l
			}
			l = append(l, peer)
		}
	}
	m.Unlock()
	selectedPeers := make(map[string]*Node)
	for _, l := range peersMap {
		selectedPeers[l[0].Name] = l[0]
	}

	answer := make([]*Node, 0)
	for _, n := range selectedPeers {
		if n.Name == m.Self.Name {
			continue
		}
		answer = append(answer, n)
	}
	return answer
}
