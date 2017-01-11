package cluster

import (
	"encoding/json"
	"math/rand"
	"strings"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
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
	Mode           ModeType
	Manager        *ClusterManager
	cfg            *memberlist.Config
	clusterPrimary = stats.NewBool("cluster.primary")
)

func Init(name, version string, started time.Time, apiScheme string, apiPort int) {
	Manager = &ClusterManager{
		Peers: make(map[string]Node),
		node: Node{
			Name:          name,
			ApiPort:       apiPort,
			ApiScheme:     apiScheme,
			Started:       started,
			Version:       version,
			Primary:       primary,
			PrimaryChange: time.Now(),
			StateChange:   time.Now(),
			Updated:       time.Now(),
			local:         true,
		},
	}
	cfg = memberlist.DefaultLANConfig()
	cfg.BindPort = clusterPort
	cfg.BindAddr = clusterHost.String()
	cfg.AdvertisePort = clusterPort
	cfg.Events = Manager
	cfg.Delegate = Manager
}

func Stop() {
	Manager.list.Leave(time.Second)
}

func Start() {
	log.Info("Starting cluster on %s:%d", cfg.BindAddr, cfg.BindPort)
	list, err := memberlist.Create(cfg)
	if err != nil {
		log.Fatal(4, "Failed to create memberlist: "+err.Error())
	}
	data, err := json.Marshal(Manager.ThisNode())
	if err != nil {
		log.Fatal(4, "Failed to marshal ThisNode metadata to json. %s", err.Error())
	}
	list.LocalNode().Meta = data

	Manager.setList(list)

	if peersStr == "" {
		return
	}
	n, err := list.Join(strings.Split(peersStr, ","))
	if err != nil {
		log.Fatal(4, "Failed to join cluster: "+err.Error())
	}
	log.Info("joined to %d nodes in cluster\n", n)
}

// return the list of nodes to broadcast requests to
// Only 1 peer per partition is returned. This list includes
// ThisNode if it is capable of handling queries.
func PeersForQuery() []Node {
	thisNode := Manager.ThisNode()
	// If we are running in single mode, just return thisNode
	if Mode == ModeSingle {
		return []Node{thisNode}
	}

	peersMap := make(map[int32][]Node)
	if thisNode.IsReady() {
		for _, part := range thisNode.Partitions {
			peersMap[part] = []Node{thisNode}
		}
	}

	for _, peer := range Manager.PeersList() {
		if !peer.IsReady() || peer.Name == thisNode.Name {
			continue
		}
		for _, part := range peer.Partitions {
			peersMap[part] = append(peersMap[part], peer)
		}
	}

	selectedPeers := make(map[string]struct{})
	answer := make([]Node, 0)
	// we want to get the minimum number of nodes
	// needed to cover all partitions

LOOP:
	for _, nodes := range peersMap {
		// always prefer the local node which will be nodes[0]
		// if it has this partition
		if nodes[0].Name == thisNode.Name {
			if _, ok := selectedPeers[thisNode.Name]; !ok {
				selectedPeers[thisNode.Name] = struct{}{}
				answer = append(answer, thisNode)
			}
			continue LOOP
		}

		for _, n := range nodes {
			if _, ok := selectedPeers[n.Name]; ok {
				continue LOOP
			}
		}
		// if no nodes have been selected yet then grab a
		// random node from the set of available nodes
		selected := nodes[rand.Intn(len(nodes))]
		selectedPeers[selected.Name] = struct{}{}
		answer = append(answer, selected)
	}

	return answer
}
