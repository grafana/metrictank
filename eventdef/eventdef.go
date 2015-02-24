package eventdef

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/codeskyblue/go-uuid"
	"github.com/ctdk/goas/v2/logger"
	elastigo "github.com/mattbaird/elastigo/lib"
	"reflect"
	"strings"
	"strconv"
	"time"
)

type EventDefinition struct {
	ID        string                 `json:"id"`
	EventType string                 `json:"event_type"`
	AccountID int64                  `json:"account_id"`
	Severity  string                 `json:"severity"` // enum "INFO" "WARN" "ERROR" "OK"
	Source    string                 `json:"source"`
	Timestamp int64                  `json:"timestamp"`
	Message   string                 `json:"message"`
	Extra     map[string]interface{} `json:"-"`
}

type RequiredField struct {
	StructName string
	Seen       bool
}

func (e *EventDefinition) UnmarshalJSON(raw []byte) error {
	//lets start by unmashaling into a basic map datastructure
	event := make(map[string]interface{})
	err := json.Unmarshal(raw, &event)
	if err != nil {
		return err
	}

	//lets get a list of our required fields.
	s := reflect.TypeOf(*e)
	requiredFields := make(map[string]*RequiredField)

	for i := 0; i < s.NumField(); i++ {
		field := s.Field(i)
		name := field.Name
		// look at the field Tags to work out the property named used in the
		// JSON document.
		tag := field.Tag.Get("json")
		if tag != "" && tag != "-" {
			name = tag
		}
		//all fields except 'Extra' are required.
		if name != "Extra" {
			requiredFields[name] = &RequiredField{
				StructName: field.Name,
				Seen:       false,
			}
		}
	}

	e.Extra = make(map[string]interface{})
	for k, v := range event {
		def, ok := requiredFields[k]
		// anything that is not a required field gets
		// stored in our 'Extra' field.
		if !ok {
			e.Extra[k] = v
		} else {
			//coerce any float64 values to int64
			if reflect.ValueOf(v).Type().Name() == "float64" {
				v = int64(v.(float64))
			}
			value := reflect.ValueOf(v)
			reflect.ValueOf(e).Elem().FieldByName(def.StructName).Set(value)
			def.Seen = true
		}
	}

	//make sure all required fields were present.
	for _, v := range requiredFields {
		if !v.Seen {
			return errors.New("Required field missing")
		}
	}
	return nil
}

func (e *EventDefinition) MarshalJSON() ([]byte, error) {
	//convert our Event object to a map[string]
	event := make(map[string]interface{})

	value := reflect.ValueOf(*e)
	for i := 0; i < value.Type().NumField(); i++ {
		field := value.Type().Field(i)
		name := field.Name
		tag := field.Tag.Get("json")
		if tag != "" && tag != "-" {
			name = tag
		}
		if name == "Extra" {
			//anything that was in Extra[] becomes a toplevel property again.
			for k, v := range e.Extra {
				event[k] = v
			}
		} else {
			v, err := encode(value.FieldByName(field.Name))
			if err != nil {
				return nil, err
			}
			event[name] = v
		}
	}
	//Marshal our map[string] into a JSON string (byte[]).
	raw, err := json.Marshal(&event)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// convert reflect.Value object to interface{}
func encode(v reflect.Value) (interface{}, error) {
	switch v.Type().Kind() {
	case reflect.Bool:
		return v.Bool(), nil
	case reflect.String:
		return v.String(), nil
	case reflect.Int64:
		return v.Int(), nil
	default:
		return nil, errors.New("Unsupported type")
	}
}

var es *elastigo.Conn

func InitElasticsearch(domain string, port int, user, pass string) error {
	es = elastigo.NewConn()
	es.Domain = domain // needs to be configurable obviously
	es.Port = strconv.Itoa(port)
	if user != "" && pass != "" {
		es.Username = user
		es.Password = pass
	}

	return nil
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
	logger.Debugf("response ok? %v", resp.Ok)
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
