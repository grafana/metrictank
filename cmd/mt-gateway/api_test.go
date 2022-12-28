package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Set up a mock http.ServeMux that returns the name of the service routed to.
// We then verify that we're routing to the expected service
func TestApi(t *testing.T) {
	mux := Api{
		ingestHandler:     stubHandler("ingest"),
		metrictankHandler: stubHandler("metrictank"),
		graphiteHandler:   stubHandler("graphite"),
		bulkImportHandler: stubHandler("bulk-import"),
	}.Mux()

	type args struct {
		path string
		want string
	}

	tests := []args{
		{
			path: "/",
			want: "graphite",
		},
		{
			path: "/whatever",
			want: "graphite",
		},
		{
			path: "/metrics/whatever",
			want: "graphite",
		},
		{
			path: "/metrics",
			want: "ingest",
		},
		{
			path: "/metrics/index.json",
			want: "metrictank",
		},
		{
			path: "/metrics/delete",
			want: "metrictank",
		},
		{
			path: "/metrics/import",
			want: "bulk-import",
		},
	}

	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			recorder := httptest.NewRecorder()

			mux.ServeHTTP(recorder, httptest.NewRequest("", test.path, nil))
			svc := recorder.Body.String()
			if svc != test.want {
				t.Errorf("want %s, got %s", test.want, svc)
			}
		})
	}

}

// creates a new http.Handler that always responds with the name of the service
func stubHandler(svc string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(svc))
	})
}
