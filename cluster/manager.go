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
	// metric cluster.events.join is how many node join events were received
	eventsJoin = stats.NewCounter32("cluster.events.join")
	// metric cluster.events.update is how many node update events were received
	eventsUpdate = stats.NewCounter32("cluster.events.update")
	// metric cluster.events.leave is how many node leave events were received
	eventsLeave = stats.NewCounter32("cluster.events.leave")

	// metric cluster.self.state.ready is whether this instance is ready
	nodeReady = stats.NewBool("cluster.self.state.ready")
	// metric cluster.self.state.primary is whether this instance is a primary
	nodePrimary = stats.NewBool("cluster.self.state.primary")
	// metric cluster.self.partitions is the number of partitions this instance consumes
	nodePartitions = stats.NewGauge32("cluster.self.partitions")
	// metric cluster.self.priority is the priority of the node. A lower number gives higher priority
	nodePriority = stats.NewGauge32("cluster.self.priority")

	// metric cluster.total.state.primary-ready is the number of nodes we know to be primary and ready
	totalPrimaryReady = stats.NewGauge32("cluster.total.state.primary-ready")
	// metric cluster.total.state.primary-not-ready is the number of nodes we know to be primary but not ready (total should only be in this state very temporarily)
	totalPrimaryNotReady = stats.NewGauge32("cluster.total.state.primary-not-ready")
	// metric cluster.total.state.secondary-ready is the number of nodes we know to be secondary and ready
	totalSecondaryReady = stats.NewGauge32("cluster.total.state.secondary-ready")
	// metric cluster.total.state.secondary-not-ready is the number of nodes we know to be secondary and not ready
	totalSecondaryNotReady = stats.NewGauge32("cluster.total.state.secondary-not-ready")
	// metric cluster.total.partitions is the number of partitions in the cluster that we know of
	totalPartitions = stats.NewGauge32("cluster.total.partitions")

	// metric cluster.decode_err.join is a counter of json unmarshal errors
	unmarshalErrJoin = stats.NewCounter32("cluster.decode_err.join")
	// metric cluster.decode_err.update is a counter of json unmarshal errors
	unmarshalErrUpdate = stats.NewCounter32("cluster.decode_err.update")
	// metric cluster.decode_err.merge_remote_state is a counter of json unmarshal errors
	unmarshalErrMergeRemoteState = stats.NewCounter32("cluster.decode_err.merge_remote_state")
)

type ClusterManager struct {
	sync.RWMutex
	members  map[string]Node // all members in the cluster, including this node.
	nodeName string
	list     *memberlist.Memberlist
}

func (c *ClusterManager) setList(list *memberlist.Memberlist) {
	c.Lock()
	c.list = list
	c.Unlock()
}

func (c *ClusterManager) ThisNode() Node {
	c.RLock()
	defer c.RUnlock()
	return c.members[c.nodeName]
}

func (c *ClusterManager) MemberList() []Node {
	c.RLock()
	list := make([]Node, len(c.members), len(c.members))
	i := 0
	for _, p := range c.members {
		list[i] = p
		i++
	}
	c.RUnlock()
	return list
}

func (c *ClusterManager) Join(peers []string) (int, error) {
	return c.list.Join(peers)
}

// report the cluster stats every time there is a change to the cluster state.
// it is assumed that the lock is acquired before calling this method.
func (c *ClusterManager) clusterStats() {
	primReady := 0
	primNotReady := 0
	secReady := 0
	secNotReady := 0
	partitions := make(map[int32]int)
	for _, p := range c.members {
		if p.Primary {
			if p.IsReady() {
				primReady++
			} else {
				primNotReady++
			}
		} else {
			if p.IsReady() {
				secReady++
			} else {
				secNotReady++
			}
		}
		for _, partition := range p.Partitions {
			partitions[partition]++
		}
	}

	totalPrimaryReady.Set(primReady)
	totalPrimaryNotReady.Set(primNotReady)
	totalSecondaryReady.Set(secReady)
	totalSecondaryNotReady.Set(secNotReady)

	totalPartitions.Set(len(partitions))
}

func (c *ClusterManager) NotifyJoin(node *memberlist.Node) {
	eventsJoin.Inc()
	c.Lock()
	defer c.Unlock()
	if len(node.Meta) == 0 {
		return
	}
	log.Info("CLU manager: Node %s with address %s has joined the cluster", node.Name, node.Addr.String())
	member := Node{}
	err := json.Unmarshal(node.Meta, &member)
	if err != nil {
		log.Error(3, "CLU manager: Failed to decode node meta from %s: %s", node.Name, err.Error())
		unmarshalErrJoin.Inc()
		return
	}
	member.RemoteAddr = node.Addr.String()
	if member.Name == c.nodeName {
		member.local = true
	}
	c.members[node.Name] = member
	c.clusterStats()
}

func (c *ClusterManager) NotifyLeave(node *memberlist.Node) {
	eventsLeave.Inc()
	c.Lock()
	defer c.Unlock()
	log.Info("CLU manager: Node %s has left the cluster", node.Name)
	delete(c.members, node.Name)
	c.clusterStats()
}

func (c *ClusterManager) NotifyUpdate(node *memberlist.Node) {
	eventsUpdate.Inc()
	c.Lock()
	defer c.Unlock()
	if len(node.Meta) == 0 {
		return
	}
	member := Node{}
	err := json.Unmarshal(node.Meta, &member)
	if err != nil {
		log.Error(3, "CLU manager: Failed to decode node meta from %s: %s", node.Name, err.Error())
		unmarshalErrUpdate.Inc()
		// if the node is known, lets mark it as notReady until it starts sending valid data again.
		if p, ok := c.members[node.Name]; ok {
			p.State = NodeNotReady
			p.StateChange = time.Now()
			// we dont set Updated as we dont want the NotReady state to propagate incase we are the only node
			// that got bad data.
			c.members[node.Name] = p
		}
		return
	}
	member.RemoteAddr = node.Addr.String()
	if member.Name == c.nodeName {
		member.local = true
	}
	c.members[node.Name] = member
	log.Info("CLU manager: Node %s at %s has been updated - %s", node.Name, node.Addr.String(), node.Meta)
	c.clusterStats()
}

func (c *ClusterManager) BroadcastUpdate() {
	if c.list != nil {
		//notify our peers immediately of the change. If this fails, which will only happen if all peers
		// are unreachable then the change will be picked up on the next complete state sync (run every 30seconds.)
		go c.list.UpdateNode(time.Second)
	}
}

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
func (c *ClusterManager) NodeMeta(limit int) []byte {
	c.RLock()
	meta, err := json.Marshal(c.members[c.nodeName])
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
	return nil
}

func (c *ClusterManager) MergeRemoteState(buf []byte, join bool) {
	return
}

// Returns true if this node is a ready to accept requests
// from users.
func (c *ClusterManager) IsReady() bool {
	c.RLock()
	defer c.RUnlock()
	return c.members[c.nodeName].IsReady()
}

// mark this node as ready to accept requests from users.
func (c *ClusterManager) SetReady() {
	c.SetState(NodeReady)
}

// Set the state of this node.
func (c *ClusterManager) SetState(state NodeState) {
	c.Lock()
	if c.members[c.nodeName].State == state {
		c.Unlock()
		return
	}
	node := c.members[c.nodeName]
	node.State = state
	node.Updated = time.Now()
	c.members[c.nodeName] = node
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
	return c.members[c.nodeName].Primary
}

// SetPrimary sets the primary status of this node
func (c *ClusterManager) SetPrimary(p bool) {
	c.Lock()
	if c.members[c.nodeName].Primary == p {
		c.Unlock()
		return
	}
	node := c.members[c.nodeName]
	node.Primary = p
	node.PrimaryChange = time.Now()
	node.Updated = time.Now()
	c.members[c.nodeName] = node
	c.Unlock()
	nodePrimary.Set(p)
	c.BroadcastUpdate()
}

// set the partitions that this node is handling.
func (c *ClusterManager) SetPartitions(part []int32) {
	c.Lock()
	node := c.members[c.nodeName]
	node.Partitions = part
	node.Updated = time.Now()
	c.members[c.nodeName] = node
	c.Unlock()
	nodePartitions.Set(len(part))
	c.BroadcastUpdate()
}

// get the partitions that this node is handling.
func (c *ClusterManager) GetPartitions() []int32 {
	c.RLock()
	defer c.RUnlock()
	return c.members[c.nodeName].Partitions
}

// set the priority of this node.
// lower values == higher priority
func (c *ClusterManager) SetPriority(prio int) {
	c.Lock()
	if c.members[c.nodeName].Priority == prio {
		c.Unlock()
		return
	}
	node := c.members[c.nodeName]
	node.Priority = prio
	node.Updated = time.Now()
	c.members[c.nodeName] = node
	c.Unlock()
	nodePriority.Set(prio)
	c.BroadcastUpdate()
}
