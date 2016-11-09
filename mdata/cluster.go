package mdata

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/raintank/worldping-api/pkg/log"
)

var (
	CluStatus *ClusterStatus
)

// ClusterStatus has Exported fields but don't touch them directly
// it's only for json marshaling. use the accessor methods.
type ClusterStatus struct {
	sync.Mutex
	Instance   string    `json:"instance"`
	Primary    bool      `json:"primary"`
	LastChange time.Time `json:"lastChange"`
}

func NewClusterStatus(instance string, initialState bool) *ClusterStatus {
	return &ClusterStatus{
		Instance:   instance,
		Primary:    initialState,
		LastChange: time.Now(),
	}
}

func (c *ClusterStatus) Marshal() ([]byte, error) {
	c.Lock()
	defer c.Unlock()
	return json.Marshal(c)
}

func (c *ClusterStatus) Set(newState bool) {
	c.Lock()
	c.Primary = newState
	c.LastChange = time.Now()
	c.Unlock()
}

func (c *ClusterStatus) IsPrimary() bool {
	c.Lock()
	defer c.Unlock()
	return c.Primary
}

// Handle requests for /cluster. POST to set primary flag, GET to get current state.
func (c *ClusterStatus) HttpHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method == "GET" {
		c.getClusterStatus(w, req)
		return
	}
	if req.Method == "POST" {
		c.setClusterStatus(w, req)
		return
	}
	http.Error(w, "not found.", http.StatusNotFound)
}

func (c *ClusterStatus) setClusterStatus(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	newState := req.Form.Get("primary")
	if newState == "" {
		http.Error(w, "primary field missing from payload.", http.StatusBadRequest)
		return
	}

	primary, err := strconv.ParseBool(newState)
	if err != nil {
		http.Error(w, "primary field could not be parsed to boolean value.", http.StatusBadRequest)
		return
	}
	c.Set(primary)
	log.Info("primary status is now %t", primary)
	w.Write([]byte("OK"))
}

func (c *ClusterStatus) getClusterStatus(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	resp, err := c.Marshal()
	if err != nil {
		http.Error(w, "could not marshal status to json", http.StatusInternalServerError)
		return
	}
	w.Write(resp)
}
