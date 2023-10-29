package api

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"time"

	_ "net/http/pprof"

	"github.com/grafana/metrictank/internal/idx"
	"github.com/grafana/metrictank/internal/mdata"
	"github.com/grafana/metrictank/internal/mdata/cache"
	"github.com/grafana/metrictank/internal/stats"
	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
	"gopkg.in/macaron.v1"
)

var (
	// metric api.get_target is how long it takes to get a target
	getTargetDuration = stats.NewLatencyHistogram15s32("api.get_target")

	// metric api.iters_to_points is how long it takes to decode points from a chunk iterator
	itersToPointsDuration = stats.NewLatencyHistogram15s32("api.iters_to_points")

	// metric api.requests_span.mem_and_cassandra is the timerange of requests hitting both in-memory and cassandra
	reqSpanBoth = stats.NewMeter32("api.requests_span.mem_and_cassandra", false)

	// metric api.requests_span.mem is the timerange of requests hitting only the ringbuffer
	reqSpanMem = stats.NewMeter32("api.requests_span.mem", false)
)

type Server struct {
	Addr            string
	SSL             bool
	certFile        string
	keyFile         string
	Macaron         *macaron.Macaron
	MetricIndex     idx.MetricIndex
	MemoryStore     mdata.Metrics
	BackendStore    mdata.Store
	MetaRecords     idx.MetaRecordIdx
	Cache           cache.Cache
	Tracer          opentracing.Tracer
	prioritySetters []PrioritySetter
	httpServer      *http.Server
}

func (s *Server) BindMetricIndex(i idx.MetricIndex) {
	s.MetricIndex = i
}
func (s *Server) BindMemoryStore(store mdata.Metrics) {
	s.MemoryStore = store
}
func (s *Server) BindBackendStore(store mdata.Store) {
	s.BackendStore = store
}
func (s *Server) BindMetaRecords(mr idx.MetaRecordIdx) {
	s.MetaRecords = mr
}

func (s *Server) BindCache(cache cache.Cache) {
	s.Cache = cache
}

func (s *Server) BindTracer(tracer opentracing.Tracer) {
	s.Tracer = tracer
}

type PrioritySetter interface {
	ExplainPriority() interface{}
}

func (s *Server) BindPrioritySetter(p PrioritySetter) {
	s.prioritySetters = append(s.prioritySetters, p)
}

func NewServer() (*Server, error) {

	m := macaron.New()
	m.Use(macaron.Recovery())
	// route pprof to where it belongs, except for our own extensions
	m.Use(func(ctx *macaron.Context) {
		if strings.HasPrefix(ctx.Req.URL.Path, "/debug/") &&
			!strings.HasPrefix(ctx.Req.URL.Path, "/debug/pprof/block") &&
			!strings.HasPrefix(ctx.Req.URL.Path, "/debug/pprof/mutex") {
			http.DefaultServeMux.ServeHTTP(ctx.Resp, ctx.Req.Request)
		}
	})

	return &Server{
		Addr:     Addr,
		SSL:      UseSSL,
		certFile: certFile,
		keyFile:  keyFile,
		Macaron:  m,
		Tracer:   opentracing.NoopTracer{},
	}, nil
}

func (s *Server) Run() {
	s.RegisterRoutes()
	proto := "http"
	if s.SSL {
		proto = "https"
	}
	log.Infof("API Listening on: %v://%s/", proto, s.Addr)

	s.httpServer = &http.Server{
		Addr:    s.Addr,
		Handler: s.Macaron,
	}

	// httpServer.Shutdown will close this listener for us
	l, err := net.Listen("tcp", s.Addr)
	if err != nil {
		log.Fatalf("API failed to listen on %s, %s", s.Addr, err.Error())
	}

	if s.SSL {
		var cert tls.Certificate
		cert, err = tls.LoadX509KeyPair(s.certFile, s.keyFile)
		if err != nil {
			log.Fatalf("API Failed to start server: %v", err)
		}
		s.httpServer.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"http/1.1"},
		}
		tlsListener := tls.NewListener(tcpKeepAliveListener{l.(*net.TCPListener)}, s.httpServer.TLSConfig)
		err = s.httpServer.Serve(tlsListener)
	} else {
		err = s.httpServer.Serve(tcpKeepAliveListener{l.(*net.TCPListener)})
	}

	if err != nil {
		log.Infof("API %s", err.Error())
	}
}

func (s *Server) Stop() {
	log.Info("API shutdown started.")
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.httpServer.Shutdown(ctx); err != nil {
			log.Printf("HTTP Server Shutdown Error: %v", err)
		}
	}
}

type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}
