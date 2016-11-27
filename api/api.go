package api

import (
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"time"

	_ "net/http/pprof"

	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/macaron.v1"
)

var LogLevel int

var (
	getTargetDuration     = stats.NewLatencyHistogram15s32("get_target_duration")
	itersToPointsDuration = stats.NewLatencyHistogram15s32("iters_to_points_duration")
	// just 1 global timer of request handling time. includes mem/cassandra gets, chunk decode/iters, json building etc
	// there is such a thing as too many metrics.  we have this, and cassandra timings, that should be enough for realtime profiling
	reqHandleDuration = stats.NewLatencyHistogram15s32("request_handle_duration")
	reqSpanBoth       = stats.NewMeter32("requests_span.mem_and_cassandra", false)
	reqSpanMem        = stats.NewMeter32("requests_span.mem", false)
)

type Server struct {
	Addr         string
	SSL          bool
	certFile     string
	keyFile      string
	Macaron      *macaron.Macaron
	MetricIndex  idx.MetricIndex
	MemoryStore  mdata.Metrics
	BackendStore mdata.Store
	shutdown     chan struct{}
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

func NewServer() (*Server, error) {

	m := macaron.New()
	m.Use(macaron.Logger())
	m.Use(macaron.Recovery())
	// route pprof to where it belongs
	m.Use(func(ctx *macaron.Context) {
		if strings.HasPrefix(ctx.Req.URL.Path, "/debug/") {
			http.DefaultServeMux.ServeHTTP(ctx.Resp, ctx.Req.Request)
		}
	})

	return &Server{
		Addr:     addr,
		SSL:      useSSL,
		certFile: certFile,
		keyFile:  keyFile,
		shutdown: make(chan struct{}),
		Macaron:  m,
	}, nil
}

func (s *Server) Run() {
	s.RegisterRoutes()
	proto := "http"
	if s.SSL {
		proto = "https"
	}
	log.Info("API Listening on: %v://%s/", proto, s.Addr)

	// define our own listner so we can call Close on it
	l, err := net.Listen("tcp", s.Addr)
	if err != nil {
		log.Fatal(4, "API failed to listen on %s, %s", s.Addr, err.Error())
	}
	go s.handleShutdown(l)
	srv := http.Server{
		Addr:    s.Addr,
		Handler: s.Macaron,
	}
	if s.SSL {
		cert, err := tls.LoadX509KeyPair(s.certFile, s.keyFile)
		if err != nil {
			log.Fatal(4, "API Failed to start server: %v", err)
		}
		srv.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"http/1.1"},
		}
		tlsListener := tls.NewListener(tcpKeepAliveListener{l.(*net.TCPListener)}, srv.TLSConfig)
		err = srv.Serve(tlsListener)
	} else {
		err = srv.Serve(tcpKeepAliveListener{l.(*net.TCPListener)})
	}

	if err != nil {
		log.Info("API %s", err.Error())
	}
}

func (s *Server) Stop() {
	close(s.shutdown)
}

func (s *Server) handleShutdown(l net.Listener) {
	<-s.shutdown
	log.Info("API shutdown started.")
	l.Close()
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
