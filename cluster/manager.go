package cluster

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
)

var (
	// total number of nodes this instance thinks are in the cluster.
	nodeCount = stats.NewGauge32("cluster.node_count")

	// number of partitions this node is consuming
	nodePartitionCount = stats.NewGauge32("cluster.node_partition_count")

	// number of partitions in the cluster this node is aware of.
	clusterPartitionCount = stats.NewGauge32("cluster.cluster_partition_count")

	// count of cluster join,update,leave events seen by this node.
	joinEvents   = stats.NewCounter32("cluster.join_events")
	updateEvents = stats.NewCounter32("cluster.update_events")
	leaveEvents  = stats.NewCounter32("cluster.leave_events")

	// the current state of this node.
	nodeReady   = stats.NewBool("cluster.ready")
	nodePrimary = stats.NewBool("cluster.primary")

	// Number of ready nodes in the cluster
	clusterReadyNodes = stats.NewGauge32("cluster.ready_nodes")
	// number of primary nodes in the cluster
	clusterPrimaryNodes = stats.NewGauge32("cluster.primary_nodes")
)

type ClusterManager struct {
	sync.RWMutex
	Peers map[string]Node
	node  Node
	list  *memberlist.Memberlist
}

func (c *ClusterManager) setList(list *memberlist.Memberlist) {
	c.Lock()
	c.list = list
	c.Unlock()
}

func (c *ClusterManager) ThisNode() Node {
	c.RLock()
	defer c.RUnlock()
	return c.node
}

func (c *ClusterManager) PeersList() []Node {
	c.RLock()
	list := make([]Node, len(c.Peers), len(c.Peers))
	i := 0
	for _, p := range c.Peers {
		list[i] = p
		i++
	}
	c.RUnlock()
	return list
}

// report the cluster stats every time there is a change to the cluster state.
// it is assumed that the lock is acquired before calling this method.
func (c *ClusterManager) clusterStats() {
	readyCount := 0
	primaryCount := 0
	partitions := make(map[int32]int)
	for _, p := range c.Peers {
		if p.Primary {
			primaryCount++
		}
		if p.IsReady() {
			readyCount++
		}
		for _, partition := range p.Partitions {
			partitions[partition]++
		}
	}

	clusterReadyNodes.Set(readyCount)
	clusterPrimaryNodes.Set(primaryCount)
	clusterPartitionCount.Set(len(partitions))
	nodeCount.Set(len(c.Peers))
}

func (c *ClusterManager) NotifyJoin(node *memberlist.Node) {
	joinEvents.Inc()
	c.Lock()
	defer c.Unlock()
	if len(node.Meta) == 0 {
		return
	}
	log.Info("CLU manager: Node %s with address %s has joined the cluster", node.Name, node.Addr.String())
	peer := Node{}
	err := json.Unmarshal(node.Meta, &peer)
	if err != nil {
		log.Error(3, "CLU manager: Failed to decode node meta from %s: %s", node.Name, err.Error())
		return
	}
	peer.RemoteAddr = node.Addr.String()
	if peer.Name == c.node.Name {
		peer.local = true
	}
	c.Peers[node.Name] = peer
	c.clusterStats()
}

func (c *ClusterManager) NotifyLeave(node *memberlist.Node) {
	leaveEvents.Inc()
	c.Lock()
	defer c.Unlock()
	log.Info("CLU manager: Node %s has left the cluster", node.Name)
	delete(c.Peers, node.Name)
	c.clusterStats()
}

func (c *ClusterManager) NotifyUpdate(node *memberlist.Node) {
	updateEvents.Inc()
	c.Lock()
	defer c.Unlock()
	if len(node.Meta) == 0 {
		return
	}
	peer := Node{}
	err := json.Unmarshal(node.Meta, &peer)
	if err != nil {
		log.Error(3, "CLU manager: Failed to decode node meta from %s: %s", node.Name, err.Error())
		// if the node is known, lets mark it as notReady until it starts sending valid data again.
		if p, ok := c.Peers[node.Name]; ok {
			p.State = NodeNotReady
			p.StateChange = time.Now()
			// we dont set Updated as we dont want the NotReady state to propagate incase we are the only node
			// that got bad data.
			c.Peers[node.Name] = p
		}
		return
	}
	peer.RemoteAddr = node.Addr.String()
	if peer.Name == c.node.Name {
		peer.local = true
	}
	c.Peers[node.Name] = peer
	log.Info("CLU manager: Node %s at %s has been updated - %s", node.Name, node.Addr.String(), node.Meta)
	c.clusterStats()
}

func (c *ClusterManager) BroadcastUpdate() {
	if c.list != nil {
		go c.list.UpdateNode(time.Second)
	}
}

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
func (c *ClusterManager) NodeMeta(limit int) []byte {
	c.RLock()
	meta, err := json.Marshal(c.node)
	c.RUnlock()
	if err != nil {
		log.Fatal(4, "CLU manager: %s", err.Error())
	}
	return meta
}

// NotifyMsg is called when a user-data message is received.
// Care should be taken that this method does not block, since doing
// so would block the entire UDP packet receive loop. Additionally, the byte
// slice may be modified after the call returns, so it should be copied if needed.
func (c *ClusterManager) NotifyMsg(buf []byte) {
	// we dont have any need for passing messages between nodes, other then
	// the NodeMeta sent with alive messages.
	return
}

// GetBroadcasts is called when user data messages can be broadcast.
// It can return a list of buffers to send. Each buffer should assume an
// overhead as provided with a limit on the total byte size allowed.
// The total byte size of the resulting data to send must not exceed
// the limit. Care should be taken that this method does not block,
// since doing so would block the entire UDP packet receive loop.
func (c *ClusterManager) GetBroadcasts(overhead, limit int) [][]byte {
	// we dont have any need for passing messages between nodes, other then
	// the NodeMeta sent with alive messages.
	return nil
}

// LocalState is used for a TCP Push/Pull. This is sent to
// the remote side in addition to the membership information. Any
// data can be sent here. See MergeRemoteState as well. The `join`
// boolean indicates this is for a join instead of a push/pull.
func (c *ClusterManager) LocalState(join bool) []byte {
	c.Lock()
	c.Peers[c.node.Name] = c.node
	meta, err := json.Marshal(c.Peers)
	c.Unlock()
	if err != nil {
		log.Fatal(4, "CLU manager: %s", err.Error())
	}
	return meta
}

func (c *ClusterManager) MergeRemoteState(buf []byte, join bool) {
	knownPeers := make(map[string]Node)
	err := json.Unmarshal(buf, &knownPeers)
	if err != nil {
		log.Error(3, "CLU manager: Unable to decode remoteState message: %s", err.Error())
		return
	}
	c.Lock()
	for name, meta := range knownPeers {
		if existing, ok := c.Peers[name]; ok {
			if meta.Updated.After(existing.Updated) {
				log.Info("CLU manager: updated node meta found in state update for %s", meta.Name)
				c.Peers[name] = meta
			}
		} else {
			log.Info("CLU manager: new node found in state update. %s", meta.Name)
			c.Peers[name] = meta
		}
	}
	c.clusterStats()
	c.Unlock()
}

// Returns true if this node is a ready to accept requests
// from users.
func (c *ClusterManager) IsReady() bool {
	c.RLock()
	defer c.RUnlock()
	return c.node.IsReady()
}

// mark this node as ready to accept requests from users.
func (c *ClusterManager) SetReady() {
	c.SetState(NodeReady)
}

// Set the state of this node.
func (c *ClusterManager) SetState(state NodeState) {
	c.Lock()
	if c.node.State == state {
		c.Unlock()
		return
	}
	c.node.State = state
	c.node.Updated = time.Now()
	c.Unlock()
	nodeReady.Set(state == NodeReady)
	c.BroadcastUpdate()
}

// mark this node as ready after the specified duration.
func (c *ClusterManager) SetReadyIn(t time.Duration) {
	go func() {
		// wait for warmupPeriod before marking ourselves
		// as ready.
		time.Sleep(t)
		c.SetReady()
	}()
}

// Returns true if the this node is a set as a primary node that should write data to cassandra.
func (c *ClusterManager) IsPrimary() bool {
	c.RLock()
	defer c.RUnlock()
	return c.node.Primary
}

// SetPrimary sets the primary status of this node
func (c *ClusterManager) SetPrimary(p bool) {
	c.Lock()
	if c.node.Primary == p {
		c.Unlock()
		return
	}
	c.node.Primary = p
	c.node.PrimaryChange = time.Now()
	c.node.Updated = time.Now()
	c.Unlock()
	nodePrimary.Set(p)
	c.BroadcastUpdate()
}

// set the partitions that this node is handling.
func (c *ClusterManager) SetPartitions(part []int32) {
	c.Lock()
	c.node.Partitions = part
	c.node.Updated = time.Now()
	c.Unlock()
	nodePartitionCount.Set(len(part))
	c.BroadcastUpdate()
}

// get the partitions that this node is handling.
func (c *ClusterManager) GetPartitions() []int32 {
	c.RLock()
	defer c.RUnlock()
	return c.node.Partitions
}
