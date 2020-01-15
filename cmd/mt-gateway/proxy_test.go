package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func Test_newSingleHostReverseProxyWithoutPrefix(t *testing.T) {
	type args struct {
		name                   string
		baseUrlSuffix          string
		prefixToDrop           string
		requestPath            string
		expectedDownstreamPath string
	}

	tests := []args{
		{
			name:                   "simple proxy",
			baseUrlSuffix:          "",
			prefixToDrop:           "",
			requestPath:            "/endpoint",
			expectedDownstreamPath: "/endpoint",
		},
		{
			name:                   "simple proxy with long base url",
			baseUrlSuffix:          "/something",
			prefixToDrop:           "",
			requestPath:            "/endpoint",
			expectedDownstreamPath: "/something/endpoint",
		},
		{
			name:                   "drop prefix",
			baseUrlSuffix:          "",
			prefixToDrop:           "/graphite",
			requestPath:            "/graphite",
			expectedDownstreamPath: "/",
		},
		{
			name:                   "drop prefix with path",
			baseUrlSuffix:          "",
			prefixToDrop:           "/graphite",
			requestPath:            "/graphite/endpoint",
			expectedDownstreamPath: "/endpoint",
		},
		{
			name:                   "drop prefix which is also in base url",
			baseUrlSuffix:          "/graphite",
			prefixToDrop:           "/graphite",
			requestPath:            "/graphite/endpoint",
			expectedDownstreamPath: "/graphite/endpoint",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			downstreamPath := "initial value"
			downstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				downstreamPath = r.URL.Path
			}))
			defer downstream.Close()
			downstreamUrl, _ := url.Parse(downstream.URL)
			downstreamUrl.Path = downstreamUrl.Path + tt.baseUrlSuffix
			proxy := newSingleHostReverseProxyWithoutPrefix(tt.prefixToDrop, downstreamUrl)

			proxy.ServeHTTP(&httptest.ResponseRecorder{}, httptest.NewRequest("GET", tt.requestPath, nil))

			if downstreamPath != tt.expectedDownstreamPath {
				t.Errorf("got %s, want %s", downstreamPath, tt.expectedDownstreamPath)
			}
		})
	}
}
