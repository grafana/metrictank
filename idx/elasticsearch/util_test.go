package elasticsearch

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"

	"gopkg.in/raintank/schema.v1"
)

func getSeriesNames(depth, count int, prefix string) []string {
	series := make([]string, count)
	for i := 0; i < count; i++ {
		ns := make([]string, depth)
		for j := 0; j < depth; j++ {
			ns[j] = getRandomString(4)
		}
		series[i] = prefix + "." + strings.Join(ns, ".")
	}
	return series
}

// source: https://github.com/gogits/gogs/blob/9ee80e3e5426821f03a4e99fad34418f5c736413/modules/base/tool.go#L58
func getRandomString(n int, alphabets ...byte) string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		if len(alphabets) == 0 {
			bytes[i] = alphanum[b%byte(len(alphanum))]
		} else {
			bytes[i] = alphabets[b%byte(len(alphabets))]
		}
	}
	return string(bytes)
}

func getMetricData(orgId, depth, count, interval int, prefix string) []*schema.MetricData {
	data := make([]*schema.MetricData, count)
	series := getSeriesNames(depth, count, prefix)
	for i, s := range series {

		data[i] = &schema.MetricData{
			Name:     s,
			Metric:   s,
			OrgId:    orgId,
			Interval: interval,
		}
		data[i].SetId()
	}
	return data
}

type mockResp struct {
	Code   int
	Body   bytes.Buffer
	header http.Header
}

func (w *mockResp) Write(b []byte) (int, error) {
	w.Body.WriteString(fmt.Sprintf("HTTP/1.1 %d %s\n", w.Code, http.StatusText(w.Code)))
	w.Body.WriteString(fmt.Sprintf("Content-Length: %d\n", len(b)))
	w.header.Write(&w.Body)
	w.Body.WriteString("\n")
	return w.Body.Write(b)
}

func (w *mockResp) WriteHeader(code int) {
	w.Code = code
}

func (w *mockResp) Header() http.Header {
	return w.header
}

type mockTransport struct {
	sync.Mutex
	Response map[string]http.HandlerFunc
}

func (t *mockTransport) Set(key string, h http.HandlerFunc) {
	t.Lock()
	t.Response[key] = h
	t.Unlock()
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		Response: make(map[string]http.HandlerFunc),
	}
}

func (t *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	fmt.Printf("MockTransport: %s %s\n", req.Method, req.URL.String())
	t.Lock()
	handler, ok := t.Response[fmt.Sprintf("%s %s", req.Method, req.URL.String())]
	t.Unlock()
	var response *http.Response

	if !ok {
		fmt.Printf("handler not found, returning 404")
		response = &http.Response{
			Header:     make(http.Header),
			Request:    req,
			StatusCode: 404,
		}
		response.Header.Set("Content-Type", "application/json")
		response.Body = ioutil.NopCloser(strings.NewReader("Not Found"))
	} else {
		w := &mockResp{header: make(http.Header)}
		handler(w, req)
		response, _ = http.ReadResponse(bufio.NewReader(&w.Body), req)
	}

	return response, nil
}

func handleOk(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(200)
	w.Write([]byte("ok"))
}

func handleCreateIndexOk(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(200)
	w.Write([]byte(`{"acknowledged":true}`))
}

func handleSearchEmpty(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(200)
	w.Write([]byte(`{"_scroll_id":"scroll","took":1,"timed_out":false,"_shards":{"total":5,"successful":5,"failed":0},"hits":{"total":0,"max_score":null,"hits":[]}}`))
}

func handleSearch1Item(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(200)
	w.Write([]byte(`{"_scroll_id":"scroll","took":1,"timed_out":false,"_shards":{"total":5,"successful":5,"failed":0},"hits":{"total":1,"max_score":1.0,"hits":[{"_index":"metric-test","_type":"metric_index","_id":"1.05023bd23b4d8c36654b0452a5ad5c65","_score":1.0,"_source":{"id":"1.05023bd23b4d8c36654b0452a5ad5c65","org_id":1,"name":"test.data","metric":"test.data","interval":0,"unit":"","mtype":"","tags":[],"lastUpdate":0,"nodes":{"n0":"test","n1":"data"},"node_count":2}}]}}`))
}

func handleBulkOk(w http.ResponseWriter, req *http.Request) {
	body, _ := ioutil.ReadAll(req.Body)
	defs := make([]string, 0)
	var buf bytes.Buffer
	bulkItem := make(map[string]interface{})
	expectingMeta := false
	for _, b := range body {
		if b == byte('\n') {
			expectingMeta = !expectingMeta
			if expectingMeta {
				if strings.Contains(buf.String(), "delete") {
					expectingMeta = !expectingMeta
				}
				buf.Reset()
				continue
			}
			err := json.Unmarshal(buf.Bytes(), &bulkItem)
			if err != nil {
				panic(err)
			}
			defs = append(defs, bulkItem["id"].(string))
			buf.Reset()
		} else {
			buf.WriteByte(b)
		}
	}
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(200)
	items := make([]map[string]interface{}, 0)
	for _, defId := range defs {
		items = append(items, map[string]interface{}{
			"index": map[string]interface{}{
				"_index":   "metrics",
				"_type":    "metric_index",
				"_version": 5,
				"status":   200,
				"_id":      defId,
			},
		})
	}

	payload := map[string]interface{}{
		"took":   1,
		"errors": false,
		"items":  items,
	}
	respBody, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	w.Write(respBody)
}

func handleBulkHalfError(w http.ResponseWriter, req *http.Request) {
	body, _ := ioutil.ReadAll(req.Body)
	defs := make([]string, 0)
	var buf bytes.Buffer
	bulkItem := make(map[string]interface{})
	expectingMeta := false
	for _, b := range body {
		if b == byte('\n') {
			expectingMeta = !expectingMeta
			if expectingMeta {
				if strings.Contains(buf.String(), "delete") {
					expectingMeta = !expectingMeta
				}
				buf.Reset()
				continue
			}
			err := json.Unmarshal(buf.Bytes(), &bulkItem)
			if err != nil {
				panic(err)
			}
			defs = append(defs, bulkItem["id"].(string))
			buf.Reset()
		} else {
			buf.WriteByte(b)
		}
	}
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(200)
	items := make([]map[string]interface{}, 0)
	hasErrors := false
	for i, defId := range defs {
		status := 200
		if (i % 2) == 1 {
			status = 503
			hasErrors = true
		}
		doc := map[string]interface{}{
			"_index":   "metrics",
			"_type":    "metric_index",
			"_version": 5,
			"status":   status,
			"_id":      defId,
		}
		if status != 200 {
			doc["error"] = "Service unavailable"
		}
		items = append(items, map[string]interface{}{
			"index": doc,
		})
	}

	payload := map[string]interface{}{
		"took":   1,
		"errors": hasErrors,
		"items":  items,
	}
	respBody, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	w.Write(respBody)
}

type requestTrace struct {
	method string
	url    string
	body   string
}
