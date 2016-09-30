package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/raintank/dur"
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

var errMetricNotFound = errors.New("metric not found")

func Delete(metricIndex idx.MetricIndex) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.FormValue("query")
		org, err := getOrg(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if query == "" {
			http.Error(w, "missing parameter `query`", http.StatusBadRequest)
			return
		}

		defs, err := metricIndex.Delete(org, query)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		resp := make(map[string]interface{})
		resp["success"] = true
		resp["deletedDefs"] = len(defs)
		b, err := json.Marshal(resp)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		writeResponse(w, b, httpTypeJSON, "")
	}
}
