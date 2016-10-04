package cluster

import (
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/raintank/worldping-api/pkg/log"
)

var ThisCluster *Manager
var ThisNode *Node

func InitManager(name, version string, primary bool, started time.Time, partitioner IdxPartitioner) {
	ThisCluster = NewManger()
	ThisCluster.Partitioner = partitioner

	ThisNode = &Node{
		Name:    name,
		Primary: primary,
		Version: version,
		State:   NodeNotReady,
		Started: started,
	}
}

type Manager struct {
	Peers          []*Node
	PartitionCount int32
	Partitioner    IdxPartitioner
	shutdown       chan struct{}
	sync.Mutex
}

func NewManger() *Manager {
	return &Manager{
		Peers:    make([]*Node, 0),
		shutdown: make(chan struct{}),
	}
}

func (m *Manager) AddPeer(remoteAddr *url.URL) {
	m.Lock()
	log.Info("adding peer with address: %s", remoteAddr.String())
	m.Peers = append(m.Peers, &Node{
		RemoteAddr: remoteAddr,
		State:      NodeNotReady,
	})
	m.Unlock()
}

func (m *Manager) GetPartitioner() IdxPartitioner {
	m.Lock()
	p := m.Partitioner
	m.Unlock()
	return p
}

func (m *Manager) SetPartitionCount(count int32) {
	m.Lock()
	m.PartitionCount = count
	m.Unlock()
}

func (m *Manager) GetPartitionCount() int32 {
	m.Lock()
	count := m.PartitionCount
	m.Unlock()
	return count
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

// return the list of peers to broadcast requests to
// Only 1 peer per partition is returned
func (m *Manager) PeersForQuery() []*Node {
	peersMap := make(map[int32][]*Node)
	for _, part := range ThisNode.GetPartitions() {
		peersMap[part] = []*Node{ThisNode}
	}
	m.Lock()
	for _, peer := range m.Peers {
		if !peer.IsReady() {
			continue
		}
		for _, part := range peer.GetPartitions() {
			_, ok := peersMap[part]
			if !ok {
				peersMap[part] = make([]*Node, 0)
			}
			peersMap[part] = append(peersMap[part], peer)
		}
	}
	m.Unlock()
	selectedPeers := make(map[string]*Node)
	for _, l := range peersMap {
		if len(l) < 1 {
			panic(fmt.Sprintf("%v", peersMap))
		}
		selectedPeers[l[0].Name] = l[0]
	}

	answer := make([]*Node, 0)
	for _, n := range selectedPeers {
		if n.Name == ThisNode.Name {
			continue
		}
		answer = append(answer, n)
	}
	return answer
}
