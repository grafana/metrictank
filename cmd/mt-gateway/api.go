package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
	"net/http/httputil"
	"net/url"
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
	//TODO implement actual kafka based import handler
	api.ingestHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotImplemented)
		_, _ = fmt.Fprintln(w, "http ingest not yet implemented")
	})
	api.graphiteHandler = defaultOrgIdMiddleware(newProxyWithLogging("graphite", urls.graphite))
	api.metrictankHandler = defaultOrgIdMiddleware(newProxyWithLogging("metrictank", urls.metrictank))
	api.bulkImportHandler = defaultOrgIdMiddleware(bulkImportHandler(urls))
	return api
}

func bulkImportHandler(urls Urls) http.Handler {
	if urls.bulkImporter.String() != "" {
		log.WithField("url", urls.bulkImporter.String()).Info("bulk importer configured")
		return newProxyWithLogging("bulk-importer", urls.bulkImporter)
	}
	log.Warn("no url configured for bulk importer service")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.WithField("service", "bulk-importer").
			WithField("method", r.Method).
			WithField("path", r.URL.Path).
			WithField("status", http.StatusServiceUnavailable).Info()
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

//Creates a new single host reverse proxy with additional logging based on the response (and service name)
func newProxyWithLogging(svc string, baseUrl *url.URL) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(baseUrl)
	proxy.ModifyResponse = func(response *http.Response) error {
		log.WithField("service", svc).
			WithField("method", response.Request.Method).
			WithField("path", response.Request.URL.Path).
			WithField("status", response.StatusCode).Info()
		return nil
	}
	return proxy
}

func defaultOrgIdMiddleware(base http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Org-Id") == "" && *defaultOrgId != "" {
			r.Header.Set("X-Org-Id", *defaultOrgId)
		}
		base.ServeHTTP(w, r)
	})
}
