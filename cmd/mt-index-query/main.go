package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/jaeger"
	"github.com/grafana/metrictank/logger"
	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
)

var (
	bootstrapNode string
	from          time.Duration
	query         string
	org           int
	logLevel      string
)

type ClusterStatus struct {
	ClusterName string             `json:"clusterName"`
	NodeName    string             `json:"nodeName"`
	Members     []cluster.HTTPNode `json:"members"`
}

func main() {
	flag.StringVar(&bootstrapNode, "bootstrap-node", "http://localhost:6060", "http address of a node in the cluster")
	flag.StringVar(&query, "query", "", "find query")
	flag.DurationVar(&from, "from", time.Hour*24, "relative from time")
	flag.IntVar(&org, "org", 1, "http address of a node in the cluster")
	flag.StringVar(&logLevel, "log-level", "info", "log level. panic|fatal|error|warning|info|debug")

	flag.Parse()
	setupLogger()
	ctx := configureTracing()
	peers, err := getNodes(bootstrapNode)
	if err != nil {
		log.Fatalf("failed to get cluster info from bootstrap node. %s", err)
	}
	if log.IsLevelEnabled(log.DebugLevel) {
		for _, peer := range peers {
			log.Debugf("%+v", peer)
		}
	}
	req := findRequest(org, query, from)
	series, err := queryPeers(ctx, peers, req)
	if err != nil {
		log.Fatalf("failed to query peers. %s", err)
	}
	for name, nodes := range series {
		if len(nodes) > 0 {
			log.Infof("%s returned %d index entries\n", name, len(nodes))
			for _, node := range nodes {
				fmt.Printf("path=%s defs=%+v\n", node.Path, node.Defs)
			}
		}
	}
}

func queryPeers(ctx context.Context, peers []cluster.HTTPNode, req models.IndexFind) (map[string][]idx.Node, error) {
	series := make(map[string][]idx.Node)
	for _, peer := range peers {
		if peer.HasData() && peer.RemoteAddr != "" {
			log.Debugf("member: %s: %s", peer.GetName(), peer.RemoteURL())
			buf, err := peer.Post(ctx, "findSeriesRemote", "/index/find", req)
			if err != nil {
				return nil, err
			}
			resp := models.IndexFindResp{}
			_, err = resp.UnmarshalMsg(buf)
			if err != nil {
				return nil, err
			}
			series[peer.Name] = make([]idx.Node, 0)
			for _, nodes := range resp.Nodes {
				series[peer.Name] = append(series[peer.Name], nodes...)
			}
		}
	}
	return series, nil
}

func findRequest(org int, query string, from time.Duration) models.IndexFind {
	fromTs := int64(0)
	if from > 0 {
		fromTs = time.Now().Add(-1 * from).Unix()
	}
	return models.IndexFind{
		Patterns: []string{query},
		OrgId:    uint32(org),
		From:     fromTs,
	}
}

func getNodes(bootstrapNode string) ([]cluster.HTTPNode, error) {
	// connect to bootstrap node and get list of peers
	resp, err := http.Get(bootstrapNode + "/cluster")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%d %s", resp.StatusCode, resp.Status)
	}
	status := ClusterStatus{}
	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&status)
	if err != nil {
		return nil, err
	}
	for i := range status.Members {
		if status.Members[i].IsLocal() {
			status.Members[i].RemoteAddr = resp.Request.URL.Hostname()
			status.Members[i].ApiPort, _ = strconv.Atoi(resp.Request.URL.Port())
			status.Members[i].ApiScheme = resp.Request.URL.Scheme
			break
		}
	}
	return status.Members, nil
}

func configureTracing() context.Context {
	jaeger.ConfigSetup()
	tracer, _, err := jaeger.Get()
	if err != nil {
		log.Fatalf("Could not initialize jaeger tracer: %s", err.Error())
	}
	cluster.Tracer = tracer
	span := tracer.StartSpan("mt-index-query")
	return opentracing.ContextWithSpan(context.Background(), span)
}

func setupLogger() {
	/***********************************
		Set up Logger
	***********************************/

	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	lvl, err := log.ParseLevel(logLevel)
	if err != nil {
		log.Fatalf("failed to parse log-level, %s", err.Error())
	}
	log.SetLevel(lvl)
	log.Infof("logging level set to '%s'", logLevel)
}
