package eventdef

import (
	"encoding/json"
	"fmt"
	"github.com/codeskyblue/go-uuid"
	elastigo "github.com/mattbaird/elastigo/lib"
	"log"
	"strings"
	"time"
)

type EventDefinition struct {
	ID        string `json:"id"`
	EventType string `json:"event_type"`
	AccountID int    `json:"account_id"`
	Severity  string `json:"severity"` // enum "INFO" "WARN" "ERROR" "OK"
	Source    string `json:"source"`
	Timestamp int64  `json:"timestamp"`
	Message   string `json:"message"`
}

var es *elastigo.Conn

func init() {
	es = elastigo.NewConn()
	es.Domain = "elasticsearch" // needs to be configurable obviously
	// TODO: once this has gotten far enough to be able to start running
	// on its own without running in tandem with the nodejs client, the
	// elasticsearch indexes will need to be checked for existence and
	// created if necessary.
}

func (e *EventDefinition) Save() error {
	if e.ID == "" {
		u := uuid.NewRandom()
		e.ID = u.String()
	}
	if e.Timestamp == 0 {
		// looks like this expects timestamps in milliseconds
		e.Timestamp = time.Now().UnixNano() / int64(time.Millisecond)
	}
	if err := e.validate(); err != nil {
		return err
	}
	resp, err := es.Index("events", e.EventType, e.ID, nil, e)
	log.Printf("response ok? %v", resp.Ok)
	if err != nil {
		return err
	}

	return nil
}

func (e *EventDefinition) validate() error {
	if e.EventType == "" || e.AccountID == 0 || e.Source == "" || e.Timestamp == 0 || e.Message == "" {
		err := fmt.Errorf("event definition not valid")
		return err
	}
	switch strings.ToLower(e.Severity) {
	case "info", "ok", "warn", "error", "warning", "critical":
		// nop
	default:
		err := fmt.Errorf("'%s' is not a valid severity level", e.Severity)
		return err
	}
	return nil
}

func EventFromJSON(b []byte) (*EventDefinition, error) {
	e := new(EventDefinition)
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, err
	}
	return e, nil
}
