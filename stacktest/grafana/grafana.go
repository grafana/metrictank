package grafana

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

var grafanaClient *http.Client

func init() {
	grafanaClient = &http.Client{}
}

type annotation struct {
	Time        int
	DashboardId int
	PanelId     int
	Tags        []string
	Text        string
}

func PostAnnotation(msg string) {
	go func() {
		a := annotation{
			Time: int(time.Now().UnixNano() / 1000 / 1000),
			Text: msg,
			Tags: []string{"metrictank"},
		}
		b, err := json.Marshal(a)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Panic("failed to marshal data")
		}
		var reader *bytes.Reader
		reader = bytes.NewReader(b)
		req, err := http.NewRequest("POST", "http://admin:admin@localhost:3000/api/annotations", reader)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Panic("request failed")
		}
		req.Header.Add("Content-Type", "application/json")
		resp, err := grafanaClient.Do(req)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Panic("response failed")
		}
		if resp.StatusCode != 200 {
			log.WithFields(log.Fields{
				"response": resp.Status,
			}).Panic("grafana annotation post response")
		}
	}()
}
