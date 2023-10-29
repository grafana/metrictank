package cluster

import (
	"errors"
	"math/rand"
	"net/http"
	"sort"
	"sync/atomic"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
)

var counter uint32

var (
	Mode    NodeMode
	Manager ClusterManager
	Tracer  opentracing.Tracer

	InsufficientShardsAvailable = NewError(http.StatusServiceUnavailable, errors.New("Insufficient shards available."))
)

func Init(name, version string, started time.Time, apiScheme string, apiPort int) {
	thisNode := HTTPNode{
		Name:          name,
		ApiPort:       apiPort,
		ApiScheme:     apiScheme,
		Started:       started,
		Version:       version,
		Primary:       primary,
		Priority:      10000,
		Mode:          Mode,
		PrimaryChange: time.Now(),
		StateChange:   time.Now(),
		Updated:       time.Now(),
		local:         true,
	}
	if Mode == ModeQuery {
		thisNode.Priority = 0
	}
	if Mode == ModeDev {
		Manager = NewSingleNodeManager(thisNode)
	} else { // Shard or Query mode
		Manager = NewMemberlistManager(thisNode)
	}
	// initialize our "primary" state metric.
	nodePrimary.Set(primary)
}

func Stop() {
	Manager.Stop()
}

func Start() {
	Manager.Start()
}

type partitionCandidates struct {
	priority int
	nodes    []Node
}

// MembersForQuery returns the list of nodes to broadcast requests to
// If partitions are assigned to nodes in groups
// (a[0,1], b[0,1], c[2,3], d[2,3] as opposed to a[0,1], b[0,2], c[1,3], d[2,3]),
// only 1 member per partition is returned.
// The nodes are selected based on priority, preferring thisNode if it
// has the lowest prio, otherwise using a random selection from all
// nodes with the lowest prio.
func MembersForQuery() ([]Node, error) {
	thisNode := Manager.ThisNode()
	// If we are running in dev mode, just return thisNode
	if Mode == ModeDev {
		return []Node{thisNode}, nil
	}

	// store the available nodes for each partition, grouped by
	// priority
	membersMap := make(map[int32]*partitionCandidates)
	if thisNode.IsReady() {
		for _, part := range thisNode.GetPartitions() {
			membersMap[part] = &partitionCandidates{
				priority: thisNode.GetPriority(),
				nodes:    []Node{thisNode},
			}
		}
	}

	for _, member := range Manager.MemberList(true, true) {
		if member.GetName() == thisNode.GetName() {
			continue
		}
		for _, part := range member.GetPartitions() {
			if _, ok := membersMap[part]; !ok {
				membersMap[part] = &partitionCandidates{
					priority: member.GetPriority(),
					nodes:    []Node{member},
				}
				continue
			}
			priority := member.GetPriority()
			if membersMap[part].priority == priority {
				membersMap[part].nodes = append(membersMap[part].nodes, member)
			} else if membersMap[part].priority > priority {
				// this node has higher priority (lower number) then previously seen candidates
				membersMap[part] = &partitionCandidates{
					priority: priority,
					nodes:    []Node{member},
				}
			}
		}
	}

	if len(membersMap) < minAvailableShards {
		return nil, InsufficientShardsAvailable
	}
	selectedMembers := make(map[string]struct{})
	answer := make([]Node, 0)
	// we want to get the minimum number of nodes
	// needed to cover all partitions

	count := int(atomic.AddUint32(&counter, 1))

LOOP:
	// for every partition...
	for _, candidates := range membersMap {

		// prefer the local node if it serves this partition
		if candidates.nodes[0].GetName() == thisNode.GetName() {
			if _, ok := selectedMembers[thisNode.GetName()]; !ok {
				selectedMembers[thisNode.GetName()] = struct{}{}
				answer = append(answer, thisNode)
			}
			continue LOOP
		}

		// for remote nodes, try to pick one we've already included

		for _, n := range candidates.nodes {
			if _, ok := selectedMembers[n.GetName()]; ok {
				continue LOOP
			}
		}

		// if no nodes have been selected yet then grab a node from
		// the set of available nodes in such a way that nodes are
		// weighted fairly across MembersForQuery calls

		selected := candidates.nodes[count%len(candidates.nodes)]
		selectedMembers[selected.GetName()] = struct{}{}
		answer = append(answer, selected)
	}

	return answer, nil
}

// MembersForSpeculativeQuery returns a prioritized list of nodes for each shard group
// keyed by the first (lowest) partition of their shard group
func MembersForSpeculativeQuery() (map[int32][]Node, error) {
	thisNode := Manager.ThisNode()
	allNodes := Manager.MemberList(true, true)
	membersMap := make(map[int32][]Node)

	// If we are running in dev mode, just return thisNode
	if Mode == ModeDev {
		membersMap[0] = []Node{thisNode}
		return membersMap, nil
	}

	peerPartitions := 0

	// store the available nodes for each partition group
	for _, member := range allNodes {
		partitions := member.GetPartitions()
		memberStartPartition := partitions[0]

		if _, ok := membersMap[memberStartPartition]; !ok {
			peerPartitions += len(partitions)
		}

		membersMap[memberStartPartition] = append(membersMap[memberStartPartition], member)
	}

	if peerPartitions < minAvailableShards {
		return nil, InsufficientShardsAvailable
	}

	for _, shard := range membersMap {
		// Shuffle to avoid always choosing the same peer first
		for i := len(shard) - 1; i > 0; i-- {
			j := rand.Intn(i + 1)
			shard[i], shard[j] = shard[j], shard[i]
		}
		sort.Slice(shard, func(i, j int) bool {
			return shard[i].GetPriority() < shard[j].GetPriority()
		})
	}

	return membersMap, nil
}
