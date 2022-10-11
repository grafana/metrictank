package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/grafana/metrictank/tracing"
	opentracing "github.com/opentracing/opentracing-go"
	tags "github.com/opentracing/opentracing-go/ext"
	log "github.com/sirupsen/logrus"
)

type InvalidNodeModeErr string

func (e InvalidNodeModeErr) Error() string {
	return fmt.Sprintf("invalid cluster operating mode %q", string(e))
}

//go:generate stringer -type=NodeMode -trimprefix=Mode
type NodeMode uint8

const (
	ModeShard NodeMode = iota
	ModeDev
	ModeQuery
)

// capitalized form is what stringer (.String()) generates and is used for json serialization
func NodeModeFromString(mode string) (NodeMode, error) {
	switch mode {
	case "single":
		log.Warn("CLU Config: 'single' mode deprecated. converting to 'dev' mode")
		return ModeDev, nil
	case "multi":
		log.Warn("CLU Config: 'multi' mode deprecated. converting to 'shard' mode")
		return ModeShard, nil
	case "dev", "Dev":
		return ModeDev, nil
	case "shard", "Shard":
		return ModeShard, nil
	case "query", "Query":
		return ModeQuery, nil
	}
	return 0, InvalidNodeModeErr(mode)
}

// MarshalJSON marshals a NodeMode
func (n NodeMode) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(n.String())
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

// UnmarshalJSON unmashals a NodeMode
func (n *NodeMode) UnmarshalJSON(b []byte) error {
	var j string
	err := json.Unmarshal(b, &j)
	if err != nil {
		return err
	}
	*n, err = NodeModeFromString(j)
	return err
}

//go:generate stringer -type=NodeState
type NodeState int

const (
	NodeNotReady NodeState = iota
	NodeReady
	NodeUnreachable
)

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

// UnmarshalJSON supports unmarshalling according to the older
// integer based, as well as the new string based, representation
func (n *NodeState) UnmarshalJSON(data []byte) error {
	s := string(data)
	switch s {
	case "0", `"NodeNotReady"`:
		*n = NodeNotReady
	case "1", `"NodeReady"`:
		*n = NodeReady
	case "2", `"NodeUnreachable"`:
		*n = NodeUnreachable
	default:
		return fmt.Errorf("unrecognized NodeState %q", s)
	}
	return nil
}

func (n NodeState) MarshalJSON() ([]byte, error) {
	switch n {
	case NodeNotReady:
		return []byte(`"NodeNotReady"`), nil
	case NodeReady:
		return []byte(`"NodeReady"`), nil
	case NodeUnreachable:
		return []byte(`"NodeUnreachable"`), nil
	}
	return nil, fmt.Errorf("impossible nodestate %v", n)
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
func (r *Error) HTTPStatusCode() int {
	return r.code
}

type HTTPNode struct {
	Name          string    `json:"name"`
	Version       string    `json:"version"`
	Primary       bool      `json:"primary"`
	PrimaryChange time.Time `json:"primaryChange"`
	Mode          NodeMode  `json:"mode"`
	State         NodeState `json:"state"`
	Priority      int       `json:"priority"`
	Started       time.Time `json:"started"`
	StateChange   time.Time `json:"stateChange"`
	Partitions    []int32   `json:"partitions"`
	ApiPort       int       `json:"apiPort"`
	ApiScheme     string    `json:"apiScheme"`
	Updated       time.Time `json:"updated"`
	RemoteAddr    string    `json:"remoteAddr"`
	local         bool
}

func (n HTTPNode) RemoteURL() string {
	return fmt.Sprintf("%s://%s:%d", n.ApiScheme, n.RemoteAddr, n.ApiPort)
}

func (n HTTPNode) IsReady() bool {
	return n.State == NodeReady && n.Priority <= maxPrio
}

func (n HTTPNode) GetPriority() int {
	return n.Priority
}

func (n HTTPNode) GetPartitions() []int32 {
	return n.Partitions
}

func (n HTTPNode) HasData() bool {
	return len(n.Partitions) > 0
}

func (n HTTPNode) IsLocal() bool {
	return n.local
}

// readyStateGCHandler adjusts the gcPercent value based on the node ready state
func (n HTTPNode) readyStateGCHandler() {
	if gcPercent == gcPercentNotReady {
		return
	}
	var err error
	if n.IsReady() {
		prev := debug.SetGCPercent(gcPercent)
		if prev != gcPercent {
			log.Infof("CLU: node is ready. changing GOGC from %d to %d", prev, gcPercent)
			err = os.Setenv("GOGC", strconv.Itoa(gcPercent))
		}
	} else {
		prev := debug.SetGCPercent(gcPercentNotReady)
		if prev != gcPercentNotReady {
			log.Infof("CLU: node is not ready. changing GOGC from %d to %d", prev, gcPercentNotReady)
			err = os.Setenv("GOGC", strconv.Itoa(gcPercentNotReady))
		}
	}
	if err != nil {
		log.Warnf("CLU: could not set GOGC environment variable. gcPercent metric will be incorrect. %s", err.Error())
	}
}

// SetState sets the state of the node and returns whether the state changed
func (n *HTTPNode) SetState(state NodeState) bool {
	if n.State == state {
		return false
	}
	n.State = state
	now := time.Now()
	n.Updated = now
	n.StateChange = now
	n.readyStateGCHandler()
	return true
}

// SetPriority sets the priority of the node and returns whether it changed
func (n *HTTPNode) SetPriority(prio int) bool {
	if n.Priority == prio {
		return false
	}
	n.Priority = prio
	n.Updated = time.Now()
	n.readyStateGCHandler()
	return true
}

// SetPrimary sets the primary state of the node and returns whether it changed
func (n *HTTPNode) SetPrimary(primary bool) bool {
	if n.Primary == primary {
		return false
	}
	now := time.Now()
	n.Primary = primary
	n.Updated = now
	n.PrimaryChange = now
	return true
}

// SetPartitions sets the partitions that this node is handling
func (n *HTTPNode) SetPartitions(part []int32) {
	n.Partitions = part
	n.Updated = time.Now()
}

func (n HTTPNode) PostRaw(ctx context.Context, name, path string, body Traceable) (io.ReadCloser, error) {
	ctx, span := tracing.NewSpan(ctx, Tracer, name)
	tags.SpanKindRPCClient.Set(span)
	tags.PeerService.Set(span, "metrictank")
	tags.PeerAddress.Set(span, n.RemoteAddr)
	tags.PeerHostname.Set(span, n.Name)
	body.Trace(span)
	var err error
	defer func(pre time.Time) {
		if err != nil {
			tags.Error.Set(span, true)
		}
		if err != nil || time.Since(pre) > 10*time.Second {
			body.TraceDebug(span)
		}
		span.Finish()
	}(time.Now())

	b, err := json.Marshal(body)
	if err != nil {
		return nil, NewError(http.StatusInternalServerError, err)
	}
	reader := bytes.NewReader(b)
	addr := n.RemoteURL() + path
	req, err := http.NewRequest("POST", addr, reader)
	if err != nil {
		return nil, NewError(http.StatusInternalServerError, err)
	}
	req = req.WithContext(ctx)
	carrier := opentracing.HTTPHeadersCarrier(req.Header)
	err = Tracer.Inject(span.Context(), opentracing.HTTPHeaders, carrier)
	if err != nil {
		log.Errorf("CLU failed to inject span into headers: %s", err.Error())
	}
	req.Header.Add("Content-Type", "application/json")
	ua := fmt.Sprintf("metrictank/%s (mode %s; state %s) Go/%s", n.Version, n.Mode.String(), n.State.String(), runtime.Version())
	req.Header.Set("User-Agent", ua)
	rsp, err := client.Do(req)

	select {
	case <-ctx.Done():
		log.Debugf("CLU HTTPNode: context canceled on request to peer %s", n.Name)
		err = nil
		return nil, nil
	default:
	}

	if err != nil {
		tags.Error.Set(span, true)
		log.Errorf("CLU HTTPNode: error trying to talk to peer %s: %s", n.Name, err.Error())
		return nil, NewError(http.StatusServiceUnavailable, errors.New("error trying to talk to peer"))
	}
	if rsp.StatusCode != 200 {
		// Read in body so that the connection can be reused
		io.Copy(ioutil.Discard, rsp.Body)
		rsp.Body.Close()
		return nil, NewError(rsp.StatusCode, fmt.Errorf(rsp.Status))
	}
	return rsp.Body, nil
}

func (n HTTPNode) Post(ctx context.Context, name, path string, body Traceable) (ret []byte, err error) {
	bodyReader, err := n.PostRaw(ctx, name, path, body)
	if err != nil || bodyReader == nil {
		return nil, err
	}
	defer bodyReader.Close()
	return ioutil.ReadAll(bodyReader)
}

func (n HTTPNode) GetName() string {
	return n.Name
}

type HTTPNodesByName []HTTPNode

func (n HTTPNodesByName) Len() int           { return len(n) }
func (n HTTPNodesByName) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n HTTPNodesByName) Less(i, j int) bool { return n[i].GetName() < n[j].GetName() }
