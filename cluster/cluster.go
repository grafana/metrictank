package cluster

import (
	"math/rand"
	"strings"
	"time"

	"github.com/hashicorp/memberlist"
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
	Mode    ModeType
	Manager *ClusterManager
	cfg     *memberlist.Config
)

func Init(name, version string, started time.Time, apiScheme string, apiPort int) {
	Manager = &ClusterManager{
		members: map[string]Node{
			name: {
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
		},
		nodeName: name,
	}
	// initialize our "primary" state metric.
	nodePrimary.Set(primary)
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
	log.Info("CLU Start: Starting cluster on %s:%d", cfg.BindAddr, cfg.BindPort)
	list, err := memberlist.Create(cfg)
	if err != nil {
		log.Fatal(4, "CLU Start: Failed to create memberlist: %s", err.Error())
	}
	Manager.setList(list)

	if peersStr == "" {
		return
	}
	n, err := list.Join(strings.Split(peersStr, ","))
	if err != nil {
		log.Fatal(4, "CLU Start: Failed to join cluster: %s", err.Error())
	}
	log.Info("CLU Start: joined to %d nodes in cluster", n)
}

// return the list of nodes to broadcast requests to
// Only 1 member per partition is returned. This list includes
// ThisNode if it is capable of handling queries.
func MembersForQuery() []Node {
	thisNode := Manager.ThisNode()
	// If we are running in single mode, just return thisNode
	if Mode == ModeSingle {
		return []Node{thisNode}
	}

	membersMap := make(map[int32][]Node)
	if thisNode.IsReady() {
		for _, part := range thisNode.Partitions {
			membersMap[part] = []Node{thisNode}
		}
	}

	for _, member := range Manager.MemberList() {
		if !member.IsReady() || member.Name == thisNode.Name {
			continue
		}
		for _, part := range member.Partitions {
			membersMap[part] = append(membersMap[part], member)
		}
	}

	selectedMembers := make(map[string]struct{})
	answer := make([]Node, 0)
	// we want to get the minimum number of nodes
	// needed to cover all partitions

LOOP:
	for _, nodes := range membersMap {
		// always prefer the local node which will be nodes[0]
		// if it has this partition
		if nodes[0].Name == thisNode.Name {
			if _, ok := selectedMembers[thisNode.Name]; !ok {
				selectedMembers[thisNode.Name] = struct{}{}
				answer = append(answer, thisNode)
			}
			continue LOOP
		}

		for _, n := range nodes {
			if _, ok := selectedMembers[n.Name]; ok {
				continue LOOP
			}
		}
		// if no nodes have been selected yet then grab a
		// random node from the set of available nodes
		selected := nodes[rand.Intn(len(nodes))]
		selectedMembers[selected.Name] = struct{}{}
		answer = append(answer, selected)
	}

	return answer
}
