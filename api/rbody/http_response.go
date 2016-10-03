package rbody

import (
	"errors"
	"github.com/raintank/metrictank/api/middleware"
)

var ErrMetricNotFound = errors.New("metric not found")

type HttpType uint

const (
	HttpTypeJSON       HttpType = iota
	HttpTypeProtobuf            = iota
	HttpTypeJavaScript          = iota
	HttpTypeRaw                 = iota
	HttpTypePickle              = iota
	HttpTypePNG                 = iota
	HttpTypeCSV                 = iota
	HttpTypeMsgp                = iota
)

func WriteResponse(w *middleware.Context, b []byte, format HttpType, jsonp string) {

	switch format {
	case HttpTypeJSON:
		if jsonp != "" {
			w.Header().Set("Content-Type", contentTypeJavaScript)
			w.Write([]byte(jsonp))
			w.Write([]byte{'('})
			w.Write(b)
			w.Write([]byte{')'})
		} else {
			w.Header().Set("Content-Type", contentTypeJSON)
			w.Write(b)
		}
	case HttpTypeProtobuf:
		w.Header().Set("Content-Type", contentTypeProtobuf)
		w.Write(b)
	case HttpTypeRaw:
		w.Header().Set("Content-Type", contentTypeRaw)
		w.Write(b)
	case HttpTypePickle:
		w.Header().Set("Content-Type", contentTypePickle)
		w.Write(b)
	case HttpTypeCSV:
		w.Header().Set("Content-Type", contentTypeCSV)
		w.Write(b)
	case HttpTypePNG:
		w.Header().Set("Content-Type", contentTypePNG)
		w.Write(b)
	case HttpTypeMsgp:
		w.Header().Set("Content-Type", contentTypeMsgp)
		w.Write(b)
	}
}

const (
	contentTypeJSON       = "application/json"
	contentTypeProtobuf   = "application/x-protobuf"
	contentTypeJavaScript = "text/javascript"
	contentTypeRaw        = "text/plain"
	contentTypePickle     = "application/pickle"
	contentTypePNG        = "image/png"
	contentTypeCSV        = "text/csv"
	contentTypeMsgp       = "application/msgpack"
)
