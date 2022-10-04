package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grafana/metrictank/cluster"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/tz"
)

func TestServer_renderMetrics_unknownFunctionError(t *testing.T) {
	initReadyCluster()
	defer initProxyStats()

	req := models.GraphiteRender{
		FromTo: tz.FromTo{
			From: "100",
			To:   "200",
			Tz:   "Europe/Madrid",
		},
		Targets: []string{"undefinedFunction(1)"},
		Format:  "json",
		NoProxy: true,
	}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	httpReq := httptest.NewRequest(http.MethodPost, "/render", bytes.NewReader(data))
	httpReq.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	server, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterRoutes()
	server.Macaron.ServeHTTP(resp, httpReq)

	// check the response
	expectedCode := http.StatusBadRequest
	if resp.Code != expectedCode {
		t.Errorf("Status code should be %d, got %d", expectedCode, resp.Code)
	}
	expectedBody := `unknown function "undefinedFunction"`
	if resp.Body.String() != expectedBody {
		t.Errorf("Body should be %q, got %q", expectedBody, resp.Body.String())
	}

	// check that unknown function stats was incremented
	if counter := proxyStats.funcMiss["undefinedFunction"]; counter == nil || counter.Peek() == 0 {
		t.Errorf("Should have accounted the funcMiss, but didn't")
	}
}

func TestServer_renderMetrics_wrongArgumentError(t *testing.T) {
	initReadyCluster()
	defer initProxyStats()

	req := models.GraphiteRender{
		FromTo: tz.FromTo{
			From: "100",
			To:   "200",
			Tz:   "Europe/Madrid",
		},
		Targets: []string{"absolute('hello')"},
		Format:  "json",
		NoProxy: true,
	}

	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	httpReq := httptest.NewRequest(http.MethodPost, "/render", bytes.NewReader(data))
	httpReq.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()

	server, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}
	server.RegisterRoutes()
	server.Macaron.ServeHTTP(resp, httpReq)

	// check the response
	expectedCode := http.StatusBadRequest
	if resp.Code != expectedCode {
		t.Errorf("Status code should be %d, got %d", expectedCode, resp.Code)
	}
	expectedBody := `can't plan function "absolute", arg 0: argument bad type. expected func or name - got etString`
	if resp.Body.String() != expectedBody {
		t.Errorf("Body should be %q, got %q", expectedBody, resp.Body.String())
	}

	// check that unknown function stats was NOT incremented
	if counter := proxyStats.funcMiss["undefinedFunction"]; counter != nil && counter.Peek() > 0 {
		t.Errorf("Should not account the funcMiss, but did")
	}
}

func TestClusterFindLimit(t *testing.T) {
	tests := []struct {
		maxSeriesPerReq int
		outstanding     int
		multiplier      int
		exp             int
		name            string
	}{
		{0, 0, 1, 0, "MSPR 0 should always result in 0"},
		{0, 1, 1, 0, "MSPR 0 should always result in 0 regardless of outstanding"},
		{0, 1, 2, 0, "MSPR 0 should always result in 0 regardless of outstanding or multiplier"},

		{10, 0, 1, 10, "MSPR 10 should result in 10 if nothing else"},
		{10, 3, 1, 7, "MSPR 10 should result in 7 if 10 outstanding"},
		{10, 3, 2, 3, "MSPR 10 should result in 3 if 10 outstanding and multiplier 2 (7/2=4 would also be reasonable. we just have to pick one)"},

		{10, 10, 1, 0, "MSPR 10 and 10 outstanding -> 0"},
		{10, 10, 2, 0, "MSPR 10 and 10 outstanding -> 0, regardless of multiplier"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getClusterFindLimit(tt.maxSeriesPerReq, tt.outstanding, tt.multiplier)
			if got != tt.exp {
				t.Errorf("got %q, want %q", got, tt.exp)
			}
		})
	}
}

func initReadyCluster() {
	cluster.Mode = cluster.ModeDev
	cluster.Init("default", "test", time.Now(), "http", 6060)
	cluster.Manager.SetReady()
	cluster.Manager.SetState(cluster.NodeReady)
	cluster.Manager.SetPriority(0)
}
