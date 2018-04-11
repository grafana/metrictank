package grafana

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
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
			panic(err)
		}
		var reader *bytes.Reader
		reader = bytes.NewReader(b)
		req, err := http.NewRequest("POST", "http://admin:admin@localhost:3000/api/annotations", reader)
		if err != nil {
			panic(err)
		}
		req.Header.Add("Content-Type", "application/json")
		resp, err := grafanaClient.Do(req)
		if err != nil {
			panic(err)
		}
		if resp.StatusCode != 200 {
			panic(fmt.Sprintf("grafana annotation post response %s", resp.Status))
		}
	}()
}
