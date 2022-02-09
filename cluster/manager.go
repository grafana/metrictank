package cluster

import (
	"crypto/sha256"
	"encoding/json"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/grafana/metrictank/stats"
	"github.com/hashicorp/memberlist"
	log "github.com/sirupsen/logrus"
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
	// metric cluster.total.state.query-ready is the number of nodes we know to be query nodes and ready
	totalQueryReady = stats.NewGauge32("cluster.total.state.query-ready")
	// metric cluster.total.state.query-not-ready is the number of nodes we know to be query nodes and not ready
	totalQueryNotReady = stats.NewGauge32("cluster.total.state.query-not-ready")
	// metric cluster.total.partitions is the number of partitions in the cluster that we know of
	totalPartitions = stats.NewGauge32("cluster.total.partitions")

	// metric cluster.decode_err.join is a counter of json unmarshal errors
	unmarshalErrJoin = stats.NewCounter32("cluster.decode_err.join")
	// metric cluster.decode_err.update is a counter of json unmarshal errors
	unmarshalErrUpdate = stats.NewCounter32("cluster.decode_err.update")
)

type ClusterManager interface {
	IsPrimary() bool
	SetPrimary(bool)
	IsReady() bool
	SetReady()
	SetState(NodeState)
	ThisNode() Node
	MemberList(bool, bool) []Node
	Join([]string) (int, error)
	GetPartitions() []int32
	SetPartitions([]int32)
	SetPriority(int)
	Stop()
	Start()
}

type MemberlistManager struct {
	sync.RWMutex
	members    map[string]HTTPNode // all members in the cluster, guaranteed to always have this node
	nodeName   string
	list       *memberlist.Memberlist
	cfg        *memberlist.Config
	prioOffset int // Used to discourage requests to come to this instance (e.g. during GC)
}

func NewMemberlistManager(thisNode HTTPNode) *MemberlistManager {
	mgr := &MemberlistManager{
		members: map[string]HTTPNode{
			thisNode.Name: thisNode,
		},
		nodeName: thisNode.Name,
	}
	switch swimUseConfig {
	case "manual":
		mgr.cfg = memberlist.DefaultLANConfig() // use this as base so that the other settings have proper defaults
		mgr.cfg.BindPort = swimBindAddr.Port
		mgr.cfg.BindAddr = swimBindAddr.IP.String()
		if swimAdvertiseAddr == nil {
			mgr.cfg.AdvertisePort = swimBindAddr.Port
		} else {
			mgr.cfg.AdvertisePort = swimAdvertiseAddr.Port
			mgr.cfg.AdvertiseAddr = swimAdvertiseAddr.IP.String()
		}
		mgr.cfg.TCPTimeout = swimTCPTimeout
		mgr.cfg.IndirectChecks = swimIndirectChecks
		mgr.cfg.RetransmitMult = swimRetransmitMult
		mgr.cfg.SuspicionMult = swimSuspicionMult
		mgr.cfg.SuspicionMaxTimeoutMult = swimSuspicionMaxTimeoutMult
		mgr.cfg.PushPullInterval = swimPushPullInterval
		mgr.cfg.ProbeInterval = swimProbeInterval
		mgr.cfg.ProbeTimeout = swimProbeTimeout
		mgr.cfg.DisableTcpPings = swimDisableTcpPings
		mgr.cfg.AwarenessMaxMultiplier = swimAwarenessMaxMultiplier
		mgr.cfg.GossipInterval = swimGossipInterval
		mgr.cfg.GossipNodes = swimGossipNodes
		mgr.cfg.GossipToTheDeadTime = swimGossipToTheDeadTime
		mgr.cfg.EnableCompression = swimEnableCompression
		mgr.cfg.DNSConfigPath = swimDNSConfigPath
	case "default-lan":
		mgr.cfg = memberlist.DefaultLANConfig()
	case "default-local":
		mgr.cfg = memberlist.DefaultLocalConfig()
	case "default-wan":
		mgr.cfg = memberlist.DefaultWANConfig()
	default:
		log.Panic("invalid swimUseConfig. should already have been validated")
	}
	mgr.cfg.Events = mgr
	mgr.cfg.Delegate = mgr
	h := sha256.New()
	h.Write([]byte(ClusterName))
	mgr.cfg.SecretKey = h.Sum(nil)

	return mgr
}

func (c *MemberlistManager) Start() {
	log.Infof("CLU Start: Starting cluster on %s:%d", c.cfg.BindAddr, c.cfg.BindPort)
	list, err := memberlist.Create(c.cfg)
	if err != nil {
		log.Fatalf("CLU Start: Failed to create memberlist: %s", err.Error())
	}
	c.setList(list)

	if gcRunInterval > 0 {
		log.Infof("CLU Start: Scheduling GC to run every %d seconds", gcRunInterval)
		go func(interval, jitter int64) {
			// jitter should not be larger than interval, normalize
			jitter = jitter % interval
			for {
				nowUnix := time.Now().Unix()
				// Align to next interval+jitter
				sleepSeconds := interval - (nowUnix % interval) + jitter
				if sleepSeconds > interval {
					sleepSeconds -= interval
				}
				time.Sleep(time.Duration(sleepSeconds) * time.Second)
				log.Infof("Triggering GC")
				c.setPrioOffset(1)
				pre := time.Now()
				runtime.GC()
				c.setPrioOffset(0)
				log.Infof("GC complete, duration = %v", time.Since(pre))
			}
		}(int64(gcRunInterval), int64(gcRunJitter))
	}

	if peersStr == "" {
		return
	}
	n, err := list.Join(strings.Split(peersStr, ","))
	if err != nil {
		log.Fatalf("CLU Start: Failed to join cluster: %s", err.Error())
	}

	log.Infof("CLU Start: joined to %d nodes in cluster", n)
}

func (c *MemberlistManager) setList(list *memberlist.Memberlist) {
	c.Lock()
	c.list = list
	c.Unlock()
}

func (c *MemberlistManager) ThisNode() Node {
	return c.thisNode()
}

func (c *MemberlistManager) thisNode() HTTPNode {
	c.RLock()
	defer c.RUnlock()
	return c.members[c.nodeName]
}

func (c *MemberlistManager) MemberList(isReady, hasData bool) []Node {
	return toIf(c.memberList(isReady, hasData))
}

func (c *MemberlistManager) memberList(isReady, hasData bool) []HTTPNode {
	c.RLock()
	list := make([]HTTPNode, 0, len(c.members))
	for _, p := range c.members {
		if isReady && !p.IsReady() {
			continue
		}
		if hasData && !p.HasData() {
			continue
		}
		list = append(list, p)
	}
	c.RUnlock()
	sort.Sort(HTTPNodesByName(list))
	return list
}

func (c *MemberlistManager) Join(peers []string) (int, error) {
	return c.list.Join(peers)
}

// report the cluster stats every time there is a change to the cluster state.
// it is assumed that the lock is acquired before calling this method.
func (c *MemberlistManager) clusterStats() {
	primReady := 0
	primNotReady := 0
	secReady := 0
	secNotReady := 0
	queryReady := 0
	queryNotReady := 0
	partitions := make(map[int32]int)
	for _, p := range c.members {
		if p.Primary {
			if p.IsReady() {
				primReady++
			} else {
				primNotReady++
			}
		} else if p.Mode != ModeQuery {
			if p.IsReady() {
				secReady++
			} else {
				secNotReady++
			}
		} else {
			if p.IsReady() {
				queryReady++
			} else {
				queryNotReady++
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
	totalQueryReady.Set(queryReady)
	totalQueryNotReady.Set(queryNotReady)

	totalPartitions.Set(len(partitions))
}

func (c *MemberlistManager) NotifyJoin(node *memberlist.Node) {
	eventsJoin.Inc()
	c.Lock()
	defer c.Unlock()
	if len(node.Meta) == 0 {
		return
	}
	log.Infof("CLU manager: HTTPNode %s with address %s has joined the cluster", node.Name, node.Addr.String())
	member := HTTPNode{}
	err := json.Unmarshal(node.Meta, &member)
	if err != nil {
		log.Errorf("CLU manager: Failed to decode node meta from %s: %s", node.Name, err.Error())
		unmarshalErrJoin.Inc()
		return
	}

	member.RemoteAddr = node.Addr.String()
	member.local = (member.Name == c.nodeName)

	// we never want anyone else in the cluster to tell us anything about ourselves
	// cause we know ourself best.
	if member.local {
		return
	}

	existing, ok := c.members[node.Name]
	if ok && !member.Updated.After(existing.Updated) {
		return
	}
	c.members[node.Name] = member
	c.clusterStats()
}

func (c *MemberlistManager) NotifyLeave(node *memberlist.Node) {
	if node.Name == c.nodeName {
		return
	}
	eventsLeave.Inc()
	c.Lock()
	defer c.Unlock()
	log.Infof("CLU manager: HTTPNode %s has left the cluster", node.Name)
	delete(c.members, node.Name)
	c.clusterStats()
}

func (c *MemberlistManager) NotifyUpdate(node *memberlist.Node) {
	eventsUpdate.Inc()
	c.Lock()
	defer c.Unlock()
	if len(node.Meta) == 0 {
		return
	}
	member := HTTPNode{}
	err := json.Unmarshal(node.Meta, &member)
	if err != nil {
		log.Errorf("CLU manager: Failed to decode node meta from %s: %s", node.Name, err.Error())
		unmarshalErrUpdate.Inc()
		// if the node is known and it is not thisNode,
		// lets mark it as notReady until it starts sending valid data again.
		if p, ok := c.members[node.Name]; ok && node.Name != c.nodeName {
			p.State = NodeNotReady
			p.StateChange = time.Now()
			// we dont set Updated as we dont want the NotReady state to propagate incase we are the only node
			// that got bad data.
			c.members[node.Name] = p
		}
		return
	}

	member.RemoteAddr = node.Addr.String()
	member.local = (member.Name == c.nodeName)

	// we never want anyone else in the cluster to tell us anything about ourselves
	// cause we know ourself best.
	if member.local {
		return
	}

	existing, ok := c.members[node.Name]
	if ok && !member.Updated.After(existing.Updated) {
		return
	}
	c.members[node.Name] = member
	log.Infof("CLU manager: HTTPNode %s at %s has been updated - %s", node.Name, node.Addr.String(), node.Meta)
	c.clusterStats()
}

func (c *MemberlistManager) BroadcastUpdate() {
	if c.list != nil {
		//notify our peers immediately of the change. If this fails, which will only happen if all peers
		// are unreachable then the change will be picked up on the next complete state sync (run every 30seconds.)
		go c.list.UpdateNode(time.Second)
	}
}

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the HTTPNode structure.
func (c *MemberlistManager) NodeMeta(limit int) []byte {
	c.RLock()
	meta, err := json.Marshal(c.members[c.nodeName])
	c.RUnlock()
	if err != nil {
		log.Fatalf("CLU manager: %s", err.Error())
	}
	return meta
}

// NotifyMsg is called when a user-data message is received.
// Care should be taken that this method does not block, since doing
// so would block the entire UDP packet receive loop. Additionally, the byte
// slice may be modified after the call returns, so it should be copied if needed.
func (c *MemberlistManager) NotifyMsg(buf []byte) {
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
func (c *MemberlistManager) GetBroadcasts(overhead, limit int) [][]byte {
	// we dont have any need for passing messages between nodes, other then
	// the NodeMeta sent with alive messages.
	return nil
}

// LocalState is used for a TCP Push/Pull. This is sent to
// the remote side in addition to the membership information. Any
// data can be sent here. See MergeRemoteState as well. The `join`
// boolean indicates this is for a join instead of a push/pull.
func (c *MemberlistManager) LocalState(join bool) []byte {
	return nil
}

func (c *MemberlistManager) MergeRemoteState(buf []byte, join bool) {
	return
}

// Returns true if this node is a ready to accept requests
// from users.
func (c *MemberlistManager) IsReady() bool {
	c.RLock()
	defer c.RUnlock()
	return c.members[c.nodeName].IsReady()
}

// mark this node as ready to accept requests from users.
func (c *MemberlistManager) SetReady() {
	c.SetState(NodeReady)
}

// Set the state of this node.
func (c *MemberlistManager) SetState(state NodeState) {
	c.Lock()
	node := c.members[c.nodeName]
	if !node.SetState(state) {
		c.Unlock()
		return
	}
	c.members[c.nodeName] = node
	c.Unlock()
	nodeReady.Set(state == NodeReady)
	c.BroadcastUpdate()
}

// Returns true if the this node is a set as a primary node that should write data to cassandra.
func (c *MemberlistManager) IsPrimary() bool {
	c.RLock()
	defer c.RUnlock()
	return c.members[c.nodeName].Primary
}

// SetPrimary sets the primary status of this node
func (c *MemberlistManager) SetPrimary(primary bool) {
	c.Lock()
	node := c.members[c.nodeName]
	if !node.SetPrimary(primary) {
		c.Unlock()
		return
	}
	c.members[c.nodeName] = node
	c.Unlock()
	nodePrimary.Set(primary)
	c.BroadcastUpdate()
}

// set the partitions that this node is handling.
func (c *MemberlistManager) SetPartitions(part []int32) {
	sort.Slice(part, func(i, j int) bool { return part[i] < part[j] })
	c.Lock()
	node := c.members[c.nodeName]
	node.SetPartitions(part)
	c.members[c.nodeName] = node
	c.Unlock()
	nodePartitions.Set(len(part))
	c.BroadcastUpdate()
}

// get the partitions that this node is handling.
func (c *MemberlistManager) GetPartitions() []int32 {
	c.RLock()
	defer c.RUnlock()
	return c.members[c.nodeName].Partitions
}

// Returns true if the this node is a set as a primary node that should write data to cassandra.
func (c *MemberlistManager) setPrioOffset(prio int) {
	c.Lock()
	c.prioOffset = prio
	c.Unlock()
}

// set the priority of this node.
// lower values == higher priority
func (c *MemberlistManager) SetPriority(prio int) {
	c.Lock()
	// Apply our universal priority offset
	prio += c.prioOffset
	node := c.members[c.nodeName]
	if !node.SetPriority(prio) {
		c.Unlock()
		return
	}
	c.members[c.nodeName] = node
	c.Unlock()
	nodePriority.Set(prio)
	c.BroadcastUpdate()
}

func (c *MemberlistManager) Stop() {
	c.list.Leave(time.Second)
}

type SingleNodeManager struct {
	sync.RWMutex
	node HTTPNode
}

func NewSingleNodeManager(thisNode HTTPNode) *SingleNodeManager {
	return &SingleNodeManager{
		node: thisNode,
	}
}

func (m *SingleNodeManager) Start() {
	return
}

func (m *SingleNodeManager) IsPrimary() bool {
	m.RLock()
	defer m.RUnlock()
	return m.node.Primary
}

func (m *SingleNodeManager) SetPrimary(primary bool) {
	m.Lock()
	m.node.SetPrimary(primary)
	m.Unlock()
	nodePrimary.Set(primary)
}

func (m *SingleNodeManager) IsReady() bool {
	m.RLock()
	defer m.RUnlock()
	return m.node.IsReady()
}

func (m *SingleNodeManager) SetReady() {
	m.SetState(NodeReady)
}

func (m *SingleNodeManager) SetState(state NodeState) {
	m.Lock()
	m.node.SetState(state)
	m.Unlock()
	nodeReady.Set(state == NodeReady)
}

func (m *SingleNodeManager) ThisNode() Node {
	m.RLock()
	defer m.RUnlock()
	return m.node
}

func (m *SingleNodeManager) MemberList(isReady, hasData bool) []Node {
	return toIf(m.memberList(isReady, hasData))
}

func (m *SingleNodeManager) memberList(isReady, hasData bool) []HTTPNode {
	m.RLock()
	defer m.RUnlock()
	if isReady && !m.node.IsReady() {
		return nil
	}
	if hasData && !m.node.HasData() {
		return nil
	}
	return []HTTPNode{m.node}
}

func (m *SingleNodeManager) Join(peers []string) (int, error) {
	//noop
	return 0, nil
}

// set the partitions that this node is handling.
func (m *SingleNodeManager) SetPartitions(part []int32) {
	sort.Slice(part, func(i, j int) bool { return part[i] < part[j] })
	m.Lock()
	m.node.SetPartitions(part)
	m.Unlock()
	nodePartitions.Set(len(part))
}

// get the partitions that this node is handling.
func (m *SingleNodeManager) GetPartitions() []int32 {
	m.RLock()
	defer m.RUnlock()
	return m.node.Partitions
}

// set the priority of this node.
// lower values == higher priority
func (m *SingleNodeManager) SetPriority(prio int) {
	m.Lock()
	m.node.SetPriority(prio)
	m.Unlock()
	nodePriority.Set(prio)
}

func (m *SingleNodeManager) Stop() {
	return
}

func toIf(in []HTTPNode) []Node {
	out := make([]Node, len(in))
	for i, m := range in {
		out[i] = m
	}
	return out
}
