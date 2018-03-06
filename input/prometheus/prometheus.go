package prometheus

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/input"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
	schema "gopkg.in/raintank/schema.v1"
)

var (
	addr        string
	Enabled     bool
	partitionID int
)

type prometheusWriteHandler struct {
	input.Handler
	quit chan struct{}
}

func New() *prometheusWriteHandler {
	return &prometheusWriteHandler{}
}

func (p *prometheusWriteHandler) Name() string {
	return "prometheus"
}

func (p *prometheusWriteHandler) Start(handler input.Handler, fatal chan struct{}) error {
	p.Handler = handler
	p.quit = fatal
	ConfigSetup()

	mux := http.NewServeMux()
	mux.HandleFunc("/write", p.handle)
	server := http.Server{
		Addr:    addr,
		Handler: mux,
	}
	go server.ListenAndServe()
	return nil
}

func (p *prometheusWriteHandler) MaintainPriority() {
	cluster.Manager.SetPriority(0)
}

func (p *prometheusWriteHandler) Stop() {
	log.Info("prometheus-in: shutting down")
}

func (p *prometheusWriteHandler) handle(w http.ResponseWriter, req *http.Request) {
	if req.Body != nil {
		defer req.Body.Close()
		compressed, err := ioutil.ReadAll(req.Body)

		if err != nil {
			w.WriteHeader(400)
			w.Write([]byte(fmt.Sprintf("Read Error, %v", err)))
			log.Error(3, "Read Error, %v", err)
			return
		}
		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			w.WriteHeader(400)
			w.Write([]byte(fmt.Sprintf("Decode Error, %v", err)))
			log.Error(3, "Decode Error, %v", err)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			w.WriteHeader(400)
			w.Write([]byte(fmt.Sprintf("Unmarshal Error, %v", err)))
			log.Error(3, "Unmarshal Error, %v", err)
			return
		}

		for _, ts := range req.Timeseries {
			var name string
			var tagSet []string

			for _, l := range ts.Labels {
				if l.Name == model.MetricNameLabel {
					name = l.Value
				} else {
					tagSet = append(tagSet, l.Name+"="+l.Value)
				}
			}
			if name != "" {
				for _, sample := range ts.Samples {
					md := &schema.MetricData{
						Name:     name,
						Metric:   name,
						Interval: 15,
						Value:    sample.Value,
						Unit:     "unknown",
						Time:     (sample.Timestamp / 1000),
						Mtype:    "gauge",
						Tags:     tagSet,
						OrgId:    1,
					}
					md.SetId()
					p.Process(md, int32(partitionID))
				}
			} else {
				w.WriteHeader(400)
				w.Write([]byte("invalid metric received: __name__ label can not equal \"\""))
				log.Warn("prometheus metric received with empty name: %v", ts.String())
				return
			}
		}
		if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}
		w.Write([]byte("ok"))
		return
	}
	w.WriteHeader(400)
	w.Write([]byte("no data"))
}

func ConfigSetup() {
	inPrometheus := flag.NewFlagSet("prometheus-in", flag.ExitOnError)
	inPrometheus.BoolVar(&Enabled, "enabled", false, "")
	inPrometheus.StringVar(&addr, "addr", ":8000", "http listen address")
	inPrometheus.IntVar(&partitionID, "partition", 0, "partition Id.")
	globalconf.Register("prometheus-in", inPrometheus)
}

func ConfigProcess() {
	if !Enabled {
		return
	}
	cluster.Manager.SetPartitions([]int32{int32(partitionID)})
}
