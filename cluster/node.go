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

//go:generate stringer -type=NodeState
type NodeState int

const (
	NodeNotReady NodeState = iota
	NodeReady
	NodeUnreachable
)

var client = http.Client{
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		Proxy:           http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout:   time.Second * 5,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: time.Second,
	},
	Timeout: time.Second,
}

func NodeStateFromString(s string) NodeState {
	switch s {
	case "NodeNotReady":
		return NodeNotReady
	case "NodeReady":
		return NodeReady
	case "NodeUnreachable":
		return NodeUnreachable
	}
	//default
	return NodeNotReady
}

type Error struct {
	code int
	err  error
}

func NewError(code int, err error) *Error {
	return &Error{
		code: code,
		err:  err,
	}
}

// implement errors.Error interface
func (r *Error) Error() string {
	return r.err.Error()
}

// implement response.Response
func (r *Error) Code() int {
	return r.code
}

type Node struct {
	sync.RWMutex
	name          string
	version       string
	started       time.Time
	remoteAddr    *url.URL
	primary       bool
	primaryChange time.Time
	state         NodeState
	stateChange   time.Time
	partitions    []int32
	probing       bool
}

func (n *Node) GetName() string {
	n.RLock()
	name := n.name
	n.RUnlock()
	return name
}

// Returns True if the node is a Primary node which
// perists data to the Backend Store (cassandra)
func (n *Node) IsPrimary() bool {
	n.RLock()
	p := n.primary
	n.RUnlock()
	return p
}

// Returns true if the node is a ready to accept requests
// from users.
func (n *Node) IsReady() bool {
	n.RLock()
	p := n.state == NodeReady
	n.RUnlock()
	return p
}

func (n *Node) SetReady() {
	n.SetState(NodeReady)
}

func (n *Node) SetReadyIn(t time.Duration) {
	go func() {
		// wait for warmupPeriod before marking ourselves
		// as ready.
		time.Sleep(t)
		n.SetReady()
	}()
}

func (n *Node) SetPrimary(p bool) {
	n.Lock()
	n.primary = p
	n.primaryChange = time.Now()
	n.Unlock()
}

func (n *Node) SetState(s NodeState) {
	n.Lock()
	n.state = s
	n.stateChange = time.Now()
	n.Unlock()
}

func (n *Node) SetPartitions(part []int32) {
	n.Lock()
	n.partitions = part
	n.Unlock()
}
func (n *Node) GetPartitions() []int32 {
	n.RLock()
	part := n.partitions
	n.RUnlock()
	return part
}

func (n *Node) Probe() {
	n.Lock()
	if n.probing {
		n.Unlock()
		return
	}
	n.probing = true
	n.Unlock()
	update := nodeJS{}
	resp, err := n.Get("/node", nil)
	n.Lock()
	defer func() {
		n.probing = false
		n.Unlock()
	}()
	if err != nil {
		log.Warn("cluster: failed to get status of peer %s. %s", n.name, err)
		if n.state != NodeUnreachable {
			n.state = NodeUnreachable
			n.stateChange = time.Now()
		}
		return
	}

	err = json.Unmarshal(resp, &update)
	if err != nil {
		log.Warn("cluster: failed to decode response from peer at address %s: %s\n\n%s", n.remoteAddr.String(), err, resp)
		if n.state != NodeNotReady {
			n.state = NodeNotReady
			n.stateChange = time.Now()
		}
		return
	}

	if n.state.String() != update.State {
		log.Info("cluster: node %s in new state: %s", update.Name, update.State)
	}

	n.name = update.Name
	n.version = update.Version
	n.started = update.Started
	n.primary = update.Primary
	n.primaryChange = update.PrimaryChange
	n.state = NodeStateFromString(update.State)
	n.stateChange = update.StateChange
	n.partitions = update.Partitions
}

func (n *Node) Get(path string, query interface{}) ([]byte, error) {
	if query != nil {
		qstr, err := toQueryString(query)
		if err != nil {
			return nil, NewError(http.StatusInternalServerError, err)
		}
		path = path + "?" + qstr
	}
	n.RLock()
	addr := n.remoteAddr.String() + path
	n.RUnlock()
	req, err := http.NewRequest("GET", addr, nil)
	if err != nil {
		return nil, NewError(http.StatusInternalServerError, err)
	}
	rsp, err := client.Do(req)
	if err != nil {
		log.Error(3, "Cluster Node: %s unreachable. %s", n.GetName(), err.Error())
		return nil, NewError(http.StatusServiceUnavailable, fmt.Errorf("cluster node unavailable"))
	}
	return handleResp(rsp)
}

func (n *Node) Post(path string, body interface{}) ([]byte, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, NewError(http.StatusInternalServerError, err)
	}
	var reader *bytes.Reader
	reader = bytes.NewReader(b)
	n.RLock()
	addr := n.remoteAddr.String() + path
	n.RUnlock()
	req, err := http.NewRequest("POST", addr, reader)
	if err != nil {
		return nil, NewError(http.StatusInternalServerError, err)
	}
	req.Header.Add("Content-Type", "application/json")
	rsp, err := client.Do(req)
	if err != nil {
		log.Error(3, "Cluster Node: %s unreachable. %s", n.GetName(), err.Error())
		return nil, NewError(http.StatusServiceUnavailable, fmt.Errorf("cluster node unavailable"))
	}
	return handleResp(rsp)
}

func handleResp(rsp *http.Response) ([]byte, error) {
	defer rsp.Body.Close()
	if rsp.StatusCode != 200 {
		return nil, NewError(rsp.StatusCode, fmt.Errorf(rsp.Status))
	}
	return ioutil.ReadAll(rsp.Body)
}

// Convert an interface{} to a urlencoded querystring
func toQueryString(q interface{}) (string, error) {
	v, err := query.Values(q)
	if err != nil {
		return "", err
	}
	return v.Encode(), nil
}

func (n *Node) toNodeJS() nodeJS {
	n.RLock()
	addr := ""
	if n.remoteAddr != nil {
		addr = n.remoteAddr.String()
	}
	njs := nodeJS{
		Name:          n.name,
		Version:       n.version,
		Primary:       n.primary,
		PrimaryChange: n.primaryChange,
		State:         n.state.String(),
		StateChange:   n.stateChange,
		Partitions:    n.partitions,
		RemoteAddress: addr,
	}
	n.RUnlock()
	return njs
}

// provide thread safe json Marshaling
func (n *Node) MarshalJSON() ([]byte, error) {
	return json.Marshal(n.toNodeJS())
}

// intermediary struct to enable thread safe json marshaling
type nodeJS struct {
	Name          string    `json:"name"`
	Version       string    `json:"version"`
	Primary       bool      `json:"primary"`
	PrimaryChange time.Time `json:"primaryChange"`
	State         string    `json:"state"`
	Started       time.Time `json:"started"`
	StateChange   time.Time `json:"stateChange"`
	Partitions    []int32   `json:"partitions"`
	RemoteAddress string    `json:"remoteAddress"`
}
