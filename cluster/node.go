package cluster

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/raintank/worldping-api/pkg/log"
)

type NodeState int

const (
	NodeReady       NodeState = 0
	NodeNotReady    NodeState = 1
	NodeUnreachable NodeState = 2
)

var NodeStateText = map[NodeState]string{
	NodeReady:       "Ready",
	NodeNotReady:    "NotReady",
	NodeUnreachable: "Unreachable",
}

var client = http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		Proxy:           http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: time.Second,
	},
	Timeout: time.Second,
}

type Node struct {
	RemoteAddr *url.URL  `json:"-"`
	Name       string    `json:"name"`
	Version    string    `json:"version"`
	Partitions []int32   `json:"partitions"`
	Primary    bool      `json:"primary"`
	State      NodeState `json:"state"`
	Started    time.Time `json:"started"`
	sync.RWMutex
}

// Returns True if the node is a Primary node which
// perists data to the Backend Store (cassandra)
func (n *Node) IsPrimary() bool {
	n.Lock()
	p := n.Primary
	n.Unlock()
	return p
}

// Returns true if the node is a ready to accept requests
// from users.
func (n *Node) IsReady() bool {
	n.Lock()
	p := n.State == NodeReady
	n.Unlock()
	return p
}

func (n *Node) SetReady() {
	n.Lock()
	n.State = NodeReady
	n.Unlock()
}

func (n *Node) SetPrimary(p bool) {
	n.Lock()
	n.Primary = p
	n.Unlock()
}

func (n *Node) SetState(s NodeState) {
	n.Lock()
	n.State = s
	n.Unlock()
}

func (n *Node) SetPartitions(part []int32) {
	n.Lock()
	n.Partitions = part
	n.Unlock()
}
func (n *Node) GetPartitions() []int32 {
	n.Lock()
	part := n.Partitions
	n.Unlock()
	return part
}

func (n *Node) Poll() {
	n.RLock()
	addr := n.RemoteAddr
	name := n.Name
	n.RUnlock()
	node, err := getPeerStatus(addr)
	if err != nil {
		log.Warn("cluster: failed to get status of peer %s. %s", name, err)
	}
	n.Lock()
	if n.State != node.State {
		if name == "" {
			name = node.Name
		}
		log.Info("cluster: node %s in new state: %s", name, NodeStateText[node.State])
	}
	n.State = node.State
	if err == nil {
		n.Name = node.Name
		n.Version = node.Version
		n.Partitions = node.Partitions
		n.Primary = node.Primary
		n.Started = node.Started
	}
	n.Unlock()
}

func getPeerStatus(addr *url.URL) (*Node, error) {
	n := &Node{
		State: NodeNotReady,
	}
	res, err := client.Get(fmt.Sprintf("%snode", addr.String()))
	if err != nil {
		log.Warn("cluster: failed to query peer at address %s: %s", addr.String(), err)
		n.State = NodeUnreachable
		return n, err
	}
	body, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		log.Warn("cluster: failed to read body from peer at address %s: %s", addr.String(), err)
		return n, err
	}
	err = json.Unmarshal(body, n)
	if err != nil {
		log.Warn("cluster: failed to decode response from peer at address %s: %s\n\n%s", addr.String(), err, body)
		n.State = NodeNotReady
		return n, err
	}
	return n, nil
}
