package cluster

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
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
	Name          string    `json:"name"`
	Version       string    `json:"version"`
	Primary       bool      `json:"primary"`
	PrimaryChange time.Time `json:"primaryChange"`
	State         NodeState `json:"state"`
	Started       time.Time `json:"started"`
	StateChange   time.Time `json:"stateChange"`
	Partitions    []int32   `json:"partitions"`
	ApiPort       int       `json:"apiPort"`
	ApiScheme     string    `json:"apiScheme"`
	Updated       time.Time `json:"updated"`
	RemoteAddr    string    `json:"remoteAddr"`
	local         bool
}

func (n Node) RemoteURL() string {
	return fmt.Sprintf("%s://%s:%d", n.ApiScheme, n.RemoteAddr, n.ApiPort)
}

func (n Node) IsReady() bool {
	return n.State == NodeReady
}

func (n Node) IsLocal() bool {
	return n.local
}

func (n Node) Get(path string, query interface{}) ([]byte, error) {
	if query != nil {
		qstr, err := toQueryString(query)
		if err != nil {
			return nil, NewError(http.StatusInternalServerError, err)
		}
		path = path + "?" + qstr
	}

	addr := n.RemoteURL() + path
	req, err := http.NewRequest("GET", addr, nil)
	if err != nil {
		return nil, NewError(http.StatusInternalServerError, err)
	}
	rsp, err := client.Do(req)
	if err != nil {
		log.Error(3, "CLU Node: %s unreachable. %s", n.Name, err.Error())
		return nil, NewError(http.StatusServiceUnavailable, fmt.Errorf("cluster node unavailable"))
	}
	return handleResp(rsp)
}

func (n Node) Post(path string, body interface{}) ([]byte, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, NewError(http.StatusInternalServerError, err)
	}
	var reader *bytes.Reader
	reader = bytes.NewReader(b)
	addr := n.RemoteURL() + path
	req, err := http.NewRequest("POST", addr, reader)
	if err != nil {
		return nil, NewError(http.StatusInternalServerError, err)
	}
	req.Header.Add("Content-Type", "application/json")
	rsp, err := client.Do(req)
	if err != nil {
		log.Error(3, "CLU Node: %s unreachable. %s", n.Name, err.Error())
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
