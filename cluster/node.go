package cluster

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/google/go-querystring/query"
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

func (n *Node) GetName() string {
	n.RLock()
	name := n.Name
	n.RUnlock()
	return name
}

// Returns True if the node is a Primary node which
// perists data to the Backend Store (cassandra)
func (n *Node) IsPrimary() bool {
	n.RLock()
	p := n.Primary
	n.RUnlock()
	return p
}

// Returns true if the node is a ready to accept requests
// from users.
func (n *Node) IsReady() bool {
	n.RLock()
	p := n.State == NodeReady
	n.RUnlock()
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
	n.RLock()
	part := n.Partitions
	n.RUnlock()
	return part
}

func (n *Node) Poll() {
	n.RLock()
	addr := n.RemoteAddr
	name := n.Name
	n.RUnlock()
	update := &Node{}
	*update = *n
	update.State = NodeNotReady

	resp, err := n.Get("/node", nil)
	n.Lock()
	if err != nil {
		log.Warn("cluster: failed to get status of peer %s. %s", name, err)
		update.State = NodeUnreachable
	} else {
		err = json.Unmarshal(resp, update)
		if err != nil {
			log.Warn("cluster: failed to decode response from peer at address %s: %s\n\n%s", addr.String(), err, resp)
			update.State = NodeNotReady
		}
	}

	if n.State != update.State {
		if name == "" {
			name = update.Name
		}
		log.Info("cluster: node %s in new state: %s", name, NodeStateText[update.State])
	}
	n.State = update.State
	if err == nil {
		n.Name = update.Name
		n.Version = update.Version
		n.Partitions = update.Partitions
		n.Primary = update.Primary
		n.Started = update.Started
	}
	n.Unlock()
}

func (n *Node) Get(path string, query interface{}) ([]byte, error) {
	if query != nil {
		qstr, err := ToQueryString(query)
		if err != nil {
			return nil, err
		}
		path = path + "?" + qstr
	}
	n.RLock()
	addr := n.RemoteAddr.String() + path
	n.RUnlock()
	req, err := http.NewRequest("GET", addr, nil)
	if err != nil {
		return nil, err
	}
	rsp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return handleResp(rsp)
}

func (n *Node) Post(path string, body interface{}) ([]byte, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	var reader *bytes.Reader
	reader = bytes.NewReader(b)
	n.RLock()
	addr := n.RemoteAddr.String() + path
	n.RUnlock()
	req, err := http.NewRequest("POST", addr, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	rsp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return handleResp(rsp)
}

func handleResp(rsp *http.Response) ([]byte, error) {
	defer rsp.Body.Close()
	if rsp.StatusCode != 200 {
		return nil, fmt.Errorf("error encountered. %s", rsp.Status)
	}
	return ioutil.ReadAll(rsp.Body)
}

// Convert an interface{} to a urlencoded querystring
func ToQueryString(q interface{}) (string, error) {
	v, err := query.Values(q)
	if err != nil {
		return "", err
	}
	return v.Encode(), nil
}
