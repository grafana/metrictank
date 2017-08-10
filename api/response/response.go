package response

import (
	"context"
	"errors"
	"net/http"

	opentracing "github.com/opentracing/opentracing-go"
	tags "github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"github.com/raintank/metrictank/util"
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

func WriteErr(ctx context.Context, w http.ResponseWriter, resp Response) {
	Write(w, resp)
	span := opentracing.SpanFromContext(ctx)
	body, _ := resp.Body()
	span.LogFields(log.String("error.kind", string(body)))
	tags.Error.Set(span, true)
}

type Response interface {
	Code() int
	Body() ([]byte, error)
	Headers() map[string]string
	Close()
}
