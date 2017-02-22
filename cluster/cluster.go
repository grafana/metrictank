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

type partitionCandidates struct {
	priority int
	nodes    []Node
}

// return the list of nodes to broadcast requests to
// If partitions are assinged to nodes in groups
// (a[0,1], b[0,1], c[2,3], d[2,3] as opposed to a[0,1], b[0,2], c[1,3], d[2,3]),
// only 1 member per partition is returned.
// The nodes are selected based on priority, prefering thisNode if it
// has the lowest prio, otherwise using a random selection from all
// nodes with the lowest prio.
func MembersForQuery() []Node {
	thisNode := Manager.ThisNode()
	// If we are running in single mode, just return thisNode
	if Mode == ModeSingle {
		return []Node{thisNode}
	}

	// store the available nodes for each partition, grouped by
	// priority
	membersMap := make(map[int32]*partitionCandidates)
	if thisNode.IsReady() {
		for _, part := range thisNode.Partitions {
			membersMap[part] = &partitionCandidates{
				priority: thisNode.Priority,
				nodes:    []Node{thisNode},
			}
		}
	}

	for _, member := range Manager.MemberList() {
		if !member.IsReady() || member.Name == thisNode.Name {
			continue
		}
		for _, part := range member.Partitions {
			if _, ok := membersMap[part]; !ok {
				membersMap[part] = &partitionCandidates{
					priority: member.Priority,
					nodes:    []Node{member},
				}
				continue
			}
			if membersMap[part].priority == member.Priority {
				membersMap[part].nodes = append(membersMap[part].nodes, member)
			} else if membersMap[part].priority > member.Priority {
				// this node has higher priority (lower number) then previously seen candidates
				membersMap[part] = &partitionCandidates{
					priority: member.Priority,
					nodes:    []Node{member},
				}
			}
		}
	}

	selectedMembers := make(map[string]struct{})
	answer := make([]Node, 0)
	// we want to get the minimum number of nodes
	// needed to cover all partitions

LOOP:
	for _, candidates := range membersMap {
		if candidates.nodes[0].Name == thisNode.Name {
			if _, ok := selectedMembers[thisNode.Name]; !ok {
				selectedMembers[thisNode.Name] = struct{}{}
				answer = append(answer, thisNode)
			}
			continue LOOP
		}

		for _, n := range candidates.nodes {
			if _, ok := selectedMembers[n.Name]; ok {
				continue LOOP
			}
		}
		// if no nodes have been selected yet then grab a
		// random node from the set of available nodes
		selected := candidates.nodes[rand.Intn(len(candidates.nodes))]
		selectedMembers[selected.Name] = struct{}{}
		answer = append(answer, selected)
	}

	return answer
}
