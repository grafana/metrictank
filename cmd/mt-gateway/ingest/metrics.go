package ingest

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"

	"github.com/golang/snappy"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/schema/msg"
	"github.com/grafana/metrictank/stats"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/publish"
	log "github.com/sirupsen/logrus"
)

var (
	metricsValid    = stats.NewCounterRate32("metrics.http.valid")    // valid metrics received (not necessarily published)
	metricsRejected = stats.NewCounterRate32("metrics.http.rejected") // invalid metrics received

	metricsTSLock    = &sync.Mutex{}
	metricsTimestamp = make(map[int]*stats.Range32)

	discardedSamples = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gateway",
			Name:      "invalid_samples_total",
			Help:      "The total number of samples that were discarded because they are invalid.",
		},
		[]string{"reason", "org"},
	)
)

func getMetricsTimestampStat(org int) *stats.Range32 {
	metricsTSLock.Lock()
	metricTimestamp, ok := metricsTimestamp[org]
	if !ok {
		metricTimestamp = stats.NewRange32(fmt.Sprintf("metrics.timestamp.http.%d", org)) // min/max timestamps seen in each interval
		metricsTimestamp[org] = metricTimestamp
	}
	metricsTSLock.Unlock()
	return metricTimestamp
}

func Metrics(w http.ResponseWriter, r *http.Request) {
	contentType := r.Header.Get("Content-Type")
	switch contentType {
	case "rt-metric-binary":
		metricsBinary(w, r, false)
	case "rt-metric-binary-snappy":
		metricsBinary(w, r, true)
	case "application/json":
		metricsJson(w, r)
	default:
		w.WriteHeader(400)
		fmt.Fprintf(w, "unknown content-type: %s", contentType)
	}
}

type discardsByReason map[string]int
type discardsByOrg map[int]discardsByReason

func (dbo discardsByOrg) Add(org int, reason string) {
	dbr, ok := dbo[org]
	if !ok {
		dbr = make(discardsByReason)
	}
	dbr[reason]++
	dbo[org] = dbr
}

func prepareIngest(ctx *models.Context, in []*schema.MetricData, toPublish []*schema.MetricData) ([]*schema.MetricData, MetricsResponse) {
	resp := NewMetricsResponse()
	promDiscards := make(discardsByOrg)

	var metricTimestamp *stats.Range32
	if !ctx.IsAdmin {
		metricTimestamp = getMetricsTimestampStat(ctx.ID)
	}

	for i, m := range in {
		if !ctx.IsAdmin {
			m.OrgId = ctx.ID
		}
		if m.Mtype == "" {
			m.Mtype = "gauge"
		}
		if err := m.Validate(); err != nil {
			log.Debugf("received invalid metric: %v %v %v", m.Name, m.OrgId, m.Tags)
			resp.AddInvalid(err, i)
			promDiscards.Add(m.OrgId, err.Error())
			continue
		}
		if ctx.IsAdmin {
			metricTimestamp = getMetricsTimestampStat(m.OrgId)
		} else {
			m.SetId()
		}
		metricTimestamp.ValueUint32(uint32(m.Time))
		toPublish = append(toPublish, m)
	}

	// track invalid/discards in graphite and prometheus
	metricsRejected.Add(resp.Invalid)
	metricsValid.Add(len(toPublish))
	for org, promDiscardsByOrg := range promDiscards {
		for reason, cnt := range promDiscardsByOrg {
			discardedSamples.WithLabelValues(reason, strconv.Itoa(org)).Add(float64(cnt))
		}
	}
	return toPublish, resp
}

func metricsJson(w http.ResponseWriter, r *http.Request) {
	if r.Body == nil {
		w.WriteHeader(400)
		fmt.Fprintf(w, "no data included in request.")
		return
	}
	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		select {
		case <-r.Context().Done():
			w.WriteHeader(499)
			fmt.Fprintf(w, "request canceled")
		default:
			log.Errorf("unable to read request body. %s", err)
			w.WriteHeader(500)
			fmt.Fprintf(w,"unable to read request body. %s", err)
		}
		return
	}
	metrics := make([]*schema.MetricData, 0)
	err = json.Unmarshal(body, &metrics)
	if err != nil {
		w.WriteHeader(400)
		fmt.Fprintf(w, "unable to parse request body. %s", err)
		return
	}

	toPublish := make([]*schema.MetricData, 0, len(metrics))
	toPublish, resp := prepareIngest(ctx, metrics, toPublish)

	select {
	case <-r.Context().Done():
		w.WriteHeader(499)
		fmt.Fprintf(w, "request canceled")
		return
	default:
	}

	err = publish.Publish(toPublish)
	if err != nil {
		log.Errorf("failed to publish metrics. %s", err)
		w.WriteHeader(500)
		fmt.Fprintf(w, "failed to publish metrics. %s", err)
		return
	}

	// track published in the response (it already has discards)
	resp.Published = len(toPublish)
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(resp)
}

func metricsBinary(w http.ResponseWriter, r *http.Request, compressed bool) {
	if r.Body == nil {
		w.WriteHeader(400)
		fmt.Fprintf(w, "no data included in request.")
		return
	}
	var bodyReadCloser io.ReadCloser
	if compressed {
		bodyReadCloser = ioutil.NopCloser(snappy.NewReader(r.Body))
	} else {
		bodyReadCloser = r.Body
	}
	defer bodyReadCloser.Close()

	body, err := ioutil.ReadAll(bodyReadCloser)
	if err != nil {
		select {
		case <-r.Context().Done():
			w.WriteHeader(499)
			fmt.Fprintf(w, "request canceled")
		default:
			log.Errorf("unable to read request body. %s", err)
			w.WriteHeader(500)
			fmt.Fprintf(w, "unable to read request body. %s", err)
		}
		return
	}
	metricData := new(msg.MetricData)
	err = metricData.InitFromMsg(body)
	if err != nil {
		log.Errorf("payload not metricData. %s", err)
		w.WriteHeader(400)
		fmt.Fprintf(w, "payload not metricData. %s", err)
		return
	}

	err = metricData.DecodeMetricData()
	if err != nil {
		log.Errorf("failed to unmarshal metricData. %s", err)
		w.WriteHeader(400)
		fmt.Fprintf(w, "failed to unmarshal metricData. %s", err)
		return
	}

	toPublish := make([]*schema.MetricData, 0, len(metricData.Metrics))
	toPublish, resp := prepareIngest(ctx, metricData.Metrics, toPublish)

	select {
	case <-r.Context().Done():
		w.WriteHeader(499)
		fmt.Fprintf(w, "request canceled")
		return
	default:
	}

	err = publish.Publish(toPublish)
	if err != nil {
		log.Errorf("failed to publish metrics. %s", err)
		w.WriteHeader(500)
		fmt.Fprintf(w,"failed to publish metrics. %s", err)
		return
	}

	// track published in the response (it already has discards)
	resp.Published = len(toPublish)
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(resp)
}
