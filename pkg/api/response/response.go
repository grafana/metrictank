package response

import (
	"errors"
	"net/http"

	"github.com/grafana/metrictank/util"
)

var ErrMetricNotFound = errors.New("metric not found")

var BufferPool = util.NewBufferPool() // used by pickle, fastjson and msgp responses to serialize into

func Write(w http.ResponseWriter, resp Response) {
	defer resp.Close()
	body, err := resp.Body()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	for k, v := range resp.Headers() {
		w.Header().Set(k, v)
	}
	w.WriteHeader(resp.Code())
	w.Write(body)
	return
}

type Response interface {
	Code() int
	Body() ([]byte, error)
	Headers() map[string]string
	Close()
}
