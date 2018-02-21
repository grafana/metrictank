package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/response"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/mdata/cache"
	"github.com/grafana/metrictank/mdata/memorystore"
	"gopkg.in/raintank/schema.v1"
)

func newSrv(delSeries, delArchives int) (*Server, *cache.MockCache) {
	srv, _ := NewServer()
	srv.RegisterRoutes()

	mdata.SetSingleAgg(conf.Avg, conf.Min, conf.Max)
	mdata.SetSingleSchema(conf.NewRetentionMT(10, 100, 600, 10, true))

	store := mdata.NewMockStore()
	store.Drop = true
	srv.BindBackendStore(store)

	mockCache := cache.NewMockCache()
	mockCache.DelMetricSeries = delSeries
	mockCache.DelMetricArchives = delArchives
	metrics := memorystore.NewAggMetrics(0, 0, 0)
	mdata.MemoryStore = metrics
	mdata.BackendStore = store
	mdata.Cache = mockCache
	srv.BindMemoryStore(metrics)
	srv.BindCache(mockCache)

	metricIndex := memory.New()
	mdata.Idx = metricIndex
	srv.BindMetricIndex(metricIndex)
	return srv, mockCache
}

func TestMetricDeleteWithPattern(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)

	delSeries := 1
	delArchives := 3
	testKey := "test.key"
	testId := "12345"

	srv, cache := newSrv(delSeries, delArchives)
	srv.MetricIndex.AddOrUpdate(
		&schema.MetricData{
			Id:       testId,
			OrgId:    1,
			Name:     testKey,
			Metric:   testKey,
			Interval: 10,
			Value:    1,
		},
		0,
	)
	req, _ := json.Marshal(models.CCacheDelete{
		Patterns:  []string{"test.*"},
		OrgId:     1,
		Propagate: false,
	})

	ts := httptest.NewServer(srv.Macaron)
	defer ts.Close()

	res, err := http.Post(ts.URL+"/ccache/delete", "application/json", bytes.NewReader(req))
	if err != nil {
		t.Fatalf("There was an error in the request: %s", err)
	}

	respParsed := models.CCacheDeleteResp{}
	buf := new(bytes.Buffer)
	buf.ReadFrom(res.Body)
	json.Unmarshal(buf.Bytes(), &respParsed)

	if len(cache.DelMetricKeys) != 1 || cache.DelMetricKeys[0] != testId {
		t.Fatalf("Expected that key %s has been deleted, but it has not", testId)
	}

	if respParsed.DeletedSeries != delSeries || respParsed.DeletedArchives != delArchives {
		t.Fatalf("Expected %d series and %d archives to get deleted, but got %d and %d", delSeries, delArchives, respParsed.DeletedSeries, respParsed.DeletedArchives)
	}
}
func TestMetricDeleteWithTags(t *testing.T) {
	_tagSupport := memory.TagSupport
	defer func() { memory.TagSupport = _tagSupport }()
	memory.TagSupport = true
	memory.TagQueryWorkers = 1

	cluster.Init("default", "test", time.Now(), "http", 6060)

	delSeries := 1
	delArchives := 3
	testKey := "test.key"
	testId := "1.01234567890123456789012345678901"

	srv, cache := newSrv(delSeries, delArchives)
	srv.MetricIndex.AddOrUpdate(
		&schema.MetricData{
			Id:       testId,
			OrgId:    1,
			Name:     testKey,
			Metric:   testKey,
			Interval: 10,
			Tags:     []string{"mytag=myvalue"},
			Value:    1,
		},
		0,
	)
	req, _ := json.Marshal(models.CCacheDelete{
		Expr:      []string{"mytag=myvalue"},
		OrgId:     1,
		Propagate: false,
	})

	ts := httptest.NewServer(srv.Macaron)
	defer ts.Close()

	res, err := http.Post(ts.URL+"/ccache/delete", "application/json", bytes.NewReader(req))
	if err != nil {
		t.Fatalf("There was an error in the request: %s", err)
	}

	respParsed := models.CCacheDeleteResp{}
	buf := new(bytes.Buffer)
	buf.ReadFrom(res.Body)
	json.Unmarshal(buf.Bytes(), &respParsed)

	if len(cache.DelMetricKeys) != 1 || cache.DelMetricKeys[0] != testId {
		t.Fatalf("Expected that key %s has been deleted, but it has not", testId)
	}

	if respParsed.DeletedSeries != delSeries || respParsed.DeletedArchives != delArchives {
		t.Fatalf("Expected %d series and %d archives to get deleted, but got %d and %d", delSeries, delArchives, respParsed.DeletedSeries, respParsed.DeletedArchives)
	}
}

func TestMetricDeleteFullFlush(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)

	req, _ := json.Marshal(models.CCacheDelete{
		Patterns:  []string{"**"},
		OrgId:     1,
		Propagate: false,
	})

	srv, cache := newSrv(0, 0)
	ts := httptest.NewServer(srv.Macaron)
	defer ts.Close()

	res, err := http.Post(ts.URL+"/ccache/delete", "application/json", bytes.NewReader(req))
	if err != nil {
		t.Fatalf("There was an error in the request: %s", err)
	}

	respParsed := models.CCacheDeleteResp{}
	buf := new(bytes.Buffer)
	buf.ReadFrom(res.Body)
	json.Unmarshal(buf.Bytes(), &respParsed)

	if cache.ResetCalls != 1 {
		t.Fatalf("Expected Reset() to have been called on the cache, but it has not")
	}
}

func TestMetricDeleteWithErrorInPropagation(t *testing.T) {
	manager := cluster.InitMock()

	// define how many series/archives are getting deleted by peer 0
	resp := models.CCacheDeleteResp{
		DeletedSeries:   1,
		DeletedArchives: 1,
		Errors:          1,
	}

	respEncoded := response.NewJson(500, resp, "")
	buf, _ := respEncoded.Body()
	manager.Peers = append(manager.Peers, cluster.NewMockNode(false, "0", buf))

	// define how many series/archives are going to get deleted by this server
	delSeries := 1
	delArchives := 3
	testKey := "test.key"
	testId := "12345"

	srv, _ := newSrv(delSeries, delArchives)
	srv.MetricIndex.AddOrUpdate(
		&schema.MetricData{
			Id:       testId,
			OrgId:    1,
			Name:     testKey,
			Metric:   testKey,
			Interval: 10,
			Value:    1,
		},
		0,
	)
	req, err := json.Marshal(models.CCacheDelete{
		Patterns:  []string{"test.*"},
		OrgId:     1,
		Propagate: true,
	})
	if err != nil {
		t.Fatalf("Unexpected error when marshaling json: %s", err)
	}

	ts := httptest.NewServer(srv.Macaron)
	defer ts.Close()

	res, err := http.Post(ts.URL+"/ccache/delete", "application/json", bytes.NewReader(req))
	if err != nil {
		t.Fatalf("There was an error in the request: %s", err)
	}

	expectedCode := 500
	if res.StatusCode != expectedCode {
		buf2 := new(bytes.Buffer)
		buf2.ReadFrom(res.Body)
		respParsed := models.CCacheDeleteResp{}
		json.Unmarshal(buf2.Bytes(), &respParsed)
		t.Fatalf("Expected status code %d, but got %d:\n%+v", expectedCode, res.StatusCode, respParsed)
	}
}

func TestMetricDeletePropagation(t *testing.T) {
	manager := cluster.InitMock()

	expectedDeletedSeries, expectedDeletedArchives := 0, 0
	for _, peer := range []string{"Peer1", "Peer2", "Peer3"} {
		// define how many series/archives are getting deleted by this peer
		resp := models.CCacheDeleteResp{
			DeletedSeries:   2,
			DeletedArchives: 5,
		}
		expectedDeletedSeries += resp.DeletedSeries
		expectedDeletedArchives += resp.DeletedArchives
		respEncoded := response.NewJson(200, resp, "")
		buf, _ := respEncoded.Body()
		manager.Peers = append(manager.Peers, cluster.NewMockNode(false, peer, buf))
	}

	// define how many series/archives are going to get deleted by this server
	delSeries := 1
	delArchives := 3
	testKey := "test.key"
	testId := "12345"

	// add up how many series/archives are expected to be deleted
	expectedDeletedSeries += delSeries
	expectedDeletedArchives += delArchives

	srv, cache := newSrv(delSeries, delArchives)
	srv.MetricIndex.AddOrUpdate(
		&schema.MetricData{
			Id:       testId,
			OrgId:    1,
			Name:     testKey,
			Metric:   testKey,
			Interval: 10,
			Value:    1,
		},
		0,
	)
	req, err := json.Marshal(models.CCacheDelete{
		Patterns:  []string{"test.*"},
		OrgId:     1,
		Propagate: true,
	})
	if err != nil {
		t.Fatalf("Unexpected error when marshaling json: %s", err)
	}

	ts := httptest.NewServer(srv.Macaron)
	defer ts.Close()

	res, err := http.Post(ts.URL+"/ccache/delete", "application/json", bytes.NewReader(req))
	if err != nil {
		t.Fatalf("There was an error in the request: %s", err)
	}

	buf2 := new(bytes.Buffer)
	buf2.ReadFrom(res.Body)
	respParsed := models.CCacheDeleteResp{}
	json.Unmarshal(buf2.Bytes(), &respParsed)

	if len(cache.DelMetricKeys) != 1 || cache.DelMetricKeys[0] != testId {
		t.Fatalf("Expected that key %s has been deleted, but it has not", testId)
	}

	deletedArchives := respParsed.DeletedArchives
	deletedSeries := respParsed.DeletedSeries
	for _, peer := range respParsed.Peers {
		deletedArchives += peer.DeletedArchives
		deletedSeries += peer.DeletedSeries
	}

	if deletedSeries != expectedDeletedSeries || deletedArchives != expectedDeletedArchives {
		t.Fatalf(
			"Expected %d series and %d archives to get deleted, but got %d and %d",
			expectedDeletedSeries, expectedDeletedArchives, respParsed.DeletedSeries, respParsed.DeletedArchives,
		)
	}
}
