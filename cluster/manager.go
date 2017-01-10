package cluster

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/raintank/worldping-api/pkg/log"
)

type ClusterManager struct {
	sync.RWMutex
	Peers map[string]Node
	node  Node
	list  *memberlist.Memberlist
}

func (c *ClusterManager) SetList(list *memberlist.Memberlist) {
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
	list := make([]Node, len(c.Peers))
	i := 0
	for _, n := range c.Peers {
		list[i] = n
		i++
	}
	c.RUnlock()
	return list
}

func (c *ClusterManager) NotifyJoin(node *memberlist.Node) {
	c.Lock()
	defer c.Unlock()
	if len(node.Meta) == 0 {
		return
	}
	log.Info("Node %s with address %s has joined the cluster\n", node.Name, node.Addr.String())
	peer := Node{}
	err := json.Unmarshal(node.Meta, &peer)
	if err != nil {
		panic(err)
	}
	peer.RemoteAddr = node.Addr.String()
	if peer.Name == c.node.Name {
		peer.local = true
	}
	c.Peers[node.Name] = peer
}

func (c *ClusterManager) NotifyLeave(node *memberlist.Node) {
	c.Lock()
	defer c.Unlock()
	log.Info("Node %s has left the cluster\n", node.Name)
	delete(c.Peers, node.Name)
}

func (c *ClusterManager) NotifyUpdate(node *memberlist.Node) {
	c.Lock()
	defer c.Unlock()
	if len(node.Meta) == 0 {
		return
	}
	peer := Node{}
	err := json.Unmarshal(node.Meta, &peer)
	if err != nil {
		panic(err)
	}
	peer.RemoteAddr = node.Addr.String()
	if peer.Name == c.node.Name {
		peer.local = true
	}
	c.Peers[node.Name] = peer
	log.Info("Node %s at %s has been updated - %s\n", node.Name, node.Addr.String(), node.Meta)
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
	c.Lock()
	meta, err := json.Marshal(c.node)
	c.Unlock()
	if err != nil {
		log.Fatal(4, err.Error())
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
		log.Fatal(4, err.Error())
	}
	return meta
}

func (c *ClusterManager) MergeRemoteState(buf []byte, join bool) {
	knownPeers := make(map[string]Node)
	err := json.Unmarshal(buf, &knownPeers)
	if err != nil {
		log.Fatal(4, err.Error())
	}
	c.Lock()
	for name, meta := range knownPeers {
		if existing, ok := c.Peers[name]; ok {
			if meta.Updated.After(existing.Updated) {
				log.Info("updated node meta found in state update for %s", meta.Name)
				c.Peers[name] = meta
			}
		} else {
			log.Info("new node found in state update. %s", meta.Name)
			c.Peers[name] = meta
		}
	}
	c.Unlock()
}

// Returns true if the node is a ready to accept requests
// from users.
func (c *ClusterManager) IsReady() bool {
	c.RLock()
	defer c.RUnlock()
	return c.node.IsReady()
}

func (c *ClusterManager) SetReady() {
	c.Lock()
	if c.node.State == NodeReady {
		c.Unlock()
		return
	}
	c.node.State = NodeReady
	c.node.Updated = time.Now()
	c.Unlock()
	c.BroadcastUpdate()
}

func (c *ClusterManager) SetReadyIn(t time.Duration) {
	go func() {
		// wait for warmupPeriod before marking ourselves
		// as ready.
		time.Sleep(t)
		c.SetReady()
	}()
}

// Returns true if the node is a set as a primary node that should write data to cassandra.
func (c *ClusterManager) IsPrimary() bool {
	c.RLock()
	defer c.RUnlock()
	return c.node.Primary
}

// SetPrimary sets the primary status.
// Note: since we set the primary metric here, this should only be called on ThisNode !
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
	clusterPrimary.Set(p)
	c.BroadcastUpdate()
}

func (c *ClusterManager) SetPartitions(part []int32) {
	c.Lock()
	c.node.Partitions = part
	c.node.Updated = time.Now()
	c.Unlock()
	c.BroadcastUpdate()
}

func (c *ClusterManager) GetPartitions() []int32 {
	c.RLock()
	defer c.RUnlock()
	return c.node.Partitions
}
