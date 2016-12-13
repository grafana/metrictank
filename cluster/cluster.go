package cluster

import (
	"math/rand"
	"net/url"
	"sync"
	"time"
)

type ModeType string

const (
	ModeSingle = "single"
	ModeMulti  = "multi"
)

func validMode(m string) bool {
	if ModeType(m) == ModeSingle || ModeType(m) == ModeMulti {
		return true
	}
	return false
}

var (
	ThisNode *Node
	Mode     ModeType
	shutdown = make(chan struct{})

	mu    sync.Mutex
	peers = make([]*Node, 0)
)

func Init(name, version string, started time.Time) {
	ThisNode = &Node{
		name:          name,
		started:       started,
		version:       version,
		primary:       false,
		primaryChange: time.Now(),
		stateChange:   time.Now(),
	}
}

func Stop() {
	close(shutdown)
}

func Start() {
	go probePeers(probeInterval)
}

func AddPeer(remoteAddr *url.URL) {
	mu.Lock()
	peer := &Node{
		remoteAddr:  remoteAddr,
		stateChange: time.Now(),
	}
	peers = append(peers, peer)
	go peer.Probe()
	mu.Unlock()
}

func GetPeers() []*Node {
	mu.Lock()
	p := make([]*Node, len(peers))
	copy(p, peers)
	mu.Unlock()
	return p
}

func probePeers(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-shutdown:
			ticker.Stop()
			return
		case <-ticker.C:
			mu.Lock()
			for _, peer := range peers {
				go peer.Probe()
			}
			mu.Unlock()
		}
	}
}

// return the list of nodes to broadcast requests to
// Only 1 peer per partition is returned. This list includes
// ThisNode if it is capable of handling queries.
func PeersForQuery() []*Node {
	// If we are running in single mode, just return thisNode
	if Mode == ModeSingle {
		return []*Node{ThisNode}
	}

	peersMap := make(map[int32][]*Node)
	if ThisNode.IsReady() {
		for _, part := range ThisNode.GetPartitions() {
			peersMap[part] = []*Node{ThisNode}
		}
	}
	mu.Lock()
	for _, peer := range peers {
		if !peer.IsReady() {
			continue
		}
		for _, part := range peer.GetPartitions() {
			peersMap[part] = append(peersMap[part], peer)
		}
	}
	mu.Unlock()
	selectedPeers := make(map[*Node]struct{})
	answer := make([]*Node, 0)
	// we want to get the minimum number of nodes
	// needed to cover all partitions
	for _, nodes := range peersMap {
		selected := nodes[0]
		// always prefer the local node which will be nodes[0]
		// if it has this partition
		if selected != ThisNode {
			// check if we are already going to use one of the
			// available nodes and re-use it
			reusePeer := false
			for _, n := range nodes {
				if _, ok := selectedPeers[n]; ok {
					selected = n
					reusePeer = true
					break
				}
			}
			// if no nodes have been selected yet then grab a
			// random node from the set of available nodes
			if !reusePeer {
				selected = nodes[rand.Intn(len(nodes))]
			}
		}

		if _, ok := selectedPeers[selected]; !ok {
			selectedPeers[selected] = struct{}{}
			answer = append(answer, selected)
		}
	}

	return answer
}
