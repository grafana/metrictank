package main

import (
	"flag"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/go-macaron/binding"
	"github.com/grafana/metrictank/cmd/mt-control-server/controlmodels"
	log "github.com/sirupsen/logrus"
	"gopkg.in/macaron.v1"
)

var (

	// TODO - config options
	// Server params
	addr    = flag.String("addr", ":6060", "Address to listen on")
	cluster = flag.String("cluster", "http://metrictank-query:6060", "URL for MT cluster")

	producer *Producer
	cass     *Cassandra
)

type Server struct {
	Addr     string
	Macaron  *macaron.Macaron
	shutdown chan struct{}
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
		Addr:     *addr,
		shutdown: make(chan struct{}),
		Macaron:  m,
	}, nil
}

func (s *Server) RegisterRoutes() {
	r := s.Macaron
	r.Use(macaron.Recovery())
	r.Use(macaron.Renderer())
	bind := binding.Bind

	r.Options("/*", func(ctx *macaron.Context) {
		ctx.Write(nil)
	})

	// SEAN TODO
	// - tag vs untagged versions?
	// - prefix '/index'?
	r.Combo("/tags/delByQuery", bind(controlmodels.IndexDelByQueryReq{})).Post(tagsDelByQuery).Get(tagsDelByQuery)
	r.Combo("/tags/restore", bind(controlmodels.IndexRestoreReq{})).Post(tagsRestore).Get(tagsRestore)
}

func (s *Server) Run() {
	s.RegisterRoutes()
	proto := "http"
	log.Infof("API Listening on: %v://%s/", proto, s.Addr)

	// define our own listener so we can call Close on it
	l, err := net.Listen("tcp", s.Addr)
	if err != nil {
		log.Fatalf("API failed to listen on %s, %s", s.Addr, err.Error())
	}
	go s.handleShutdown(l)
	srv := http.Server{
		Addr:    s.Addr,
		Handler: s.Macaron,
	}

	err = srv.Serve(tcpKeepAliveListener{l.(*net.TCPListener)})

	if err != nil {
		log.Infof("API %s", err.Error())
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

func main() {
	apiServer, err := NewServer()
	if err != nil {
		log.Fatalf("Failed to start API. %s", err.Error())
	}

	producer = NewProducer()
	cass, err = NewCassandra()
	if err != nil {
		log.Fatalf("Failed to config cassandra. %s", err.Error())
	}

	apiServer.Run()
	// TODO - handle shutdown somehow
}
