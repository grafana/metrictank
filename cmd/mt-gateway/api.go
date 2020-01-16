package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
	"net/http/httputil"
	"strconv"
)

//Maintains a set of `http.Handlers` for the different API endpoints.
//Used to generate an http.ServeMux via `api.Mux()`
type Api struct {
	ingestHandler     http.Handler
	metrictankHandler http.Handler
	graphiteHandler   http.Handler
	bulkImportHandler http.Handler
}

//Constructs a new Api based on the passed in URLS
func NewApi(urls Urls) Api {
	api := Api{}
	api.ingestHandler = withMiddleware("ingest", ingestHandlerStub)
	api.graphiteHandler = withMiddleware("graphite", httputil.NewSingleHostReverseProxy(urls.graphite))
	api.metrictankHandler = withMiddleware("metrictank", httputil.NewSingleHostReverseProxy(urls.metrictank))
	api.bulkImportHandler = withMiddleware("bulk-importer", bulkImportHandler(urls))
	return api
}

//TODO replace this with an actual implementation
var ingestHandlerStub = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	_, _ = fmt.Fprintln(w, "http ingest not yet implemented")
})

//Returns a proxy to the bulk importer if one is configured, otherwise a handler that always returns a 503
func bulkImportHandler(urls Urls) http.Handler {
	if urls.bulkImporter.String() != "" {
		log.WithField("url", urls.bulkImporter.String()).Info("bulk importer configured")
		return httputil.NewSingleHostReverseProxy(urls.bulkImporter)
	}
	log.Info("no url configured for bulk importer service, disabling")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintln(w, "no url configured for bulk importer service")
	})
}

//Builds an http.ServeMux based on the handlers defined in the Api
func (api Api) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	//By default everything is proxied to graphite
	//This includes endpoints under `/metrics` which aren't explicitly rerouted
	mux.Handle("/", api.graphiteHandler)
	//`/metrics` is handled locally by the kafka ingester (not yet implemented)
	mux.Handle("/metrics", api.ingestHandler)
	//other endpoints are proxied to metrictank or mt-whisper-import-writer
	mux.Handle("/metrics/index.json", api.metrictankHandler)
	mux.Handle("/metrics/delete", api.metrictankHandler)
	mux.Handle("/metrics/import", api.bulkImportHandler)

	return mux
}

//Add logging and default orgId middleware to the http handler
func withMiddleware(svc string, base http.Handler) http.Handler {
	return defaultOrgIdMiddleware(loggingMiddleware(svc, base))
}

//http.ResponseWriter that saves the status code
type statusRecorder struct {
	http.ResponseWriter
	status int
}

//delegate to the main response writer, but save the code
func (rec *statusRecorder) WriteHeader(code int) {
	rec.status = code
	rec.ResponseWriter.WriteHeader(code)
}

//add request logging to the given handler
func loggingMiddleware(svc string, base http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		recorder := statusRecorder{w, -1}
		base.ServeHTTP(&recorder, request)
		log.WithField("service", svc).
			WithField("method", request.Method).
			WithField("path", request.URL.Path).
			WithField("status", recorder.status).Info()
	})
}

//Set the `X-Org-Id` header to the default if there is not one present
func defaultOrgIdMiddleware(base http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Org-Id") == "" && *defaultOrgId != -1 {
			r.Header.Set("X-Org-Id", strconv.Itoa(*defaultOrgId))
		}
		base.ServeHTTP(w, r)
	})
}
