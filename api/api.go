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
	"github.com/raintank/metrictank/mdata/cache"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/macaron.v1"
)

var LogLevel int

var (
	// metric api.get_target is how long it takes to get a target
	getTargetDuration = stats.NewLatencyHistogram15s32("api.get_target")

	// metric api.iters_to_points is how long it takes to decode points from a chunk iterator
	itersToPointsDuration = stats.NewLatencyHistogram15s32("api.iters_to_points")

	// metric api.request_handle is how long it takes to handle a render request
	reqHandleDuration = stats.NewLatencyHistogram15s32("api.request_handle")

	// metric api.requests_span.mem_and_cassandra is the timerange of requests hitting both in-memory and cassandra
	reqSpanBoth = stats.NewMeter32("api.requests_span.mem_and_cassandra", false)

	// metric api.requests_span.mem is the timerange of requests hitting only the ringbuffer
	reqSpanMem = stats.NewMeter32("api.requests_span.mem", false)
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
	Cache        cache.Cache
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

func (s *Server) BindCache(cache cache.Cache) {
	s.Cache = cache
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
		Addr:     Addr,
		SSL:      UseSSL,
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
