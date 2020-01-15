package main

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

//Create a new single host reverse proxy that drops the given prefix from the URL sent to the downstream.
//For instance, a request to /graphite/endpoint with a dropped prefix of "/graphite" will send "/endpoint" to the downstream server
func newSingleHostReverseProxyWithoutPrefix(prefix string, baseUrl *url.URL) *httputil.ReverseProxy {
	proxy := httputil.NewSingleHostReverseProxy(baseUrl)
	original := proxy.Director
	//We want to do what the original director function did, then trim out the prefix
	proxy.Director = func(request *http.Request) {
		original(request)
		fullPath := request.URL.Path
		portionAfterBasePath := strings.TrimPrefix(fullPath, baseUrl.Path)
		portionAfterBasePathWithoutPrefix := strings.TrimPrefix(portionAfterBasePath, prefix)
		request.URL.Path = baseUrl.Path + portionAfterBasePathWithoutPrefix
	}
	return proxy
}
