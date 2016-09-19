package main

import (
	"net/http"
)

type httpType uint

const (
	httpTypeJSON       httpType = iota
	httpTypeProtobuf            = iota
	httpTypeJavaScript          = iota
	httpTypeRaw                 = iota
	httpTypePickle              = iota
	httpTypePNG                 = iota
	httpTypeCSV                 = iota
	httpTypeMsgp                = iota
)

func writeResponse(w http.ResponseWriter, b []byte, format httpType, jsonp string) {

	switch format {
	case httpTypeJSON:
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
	case httpTypeProtobuf:
		w.Header().Set("Content-Type", contentTypeProtobuf)
		w.Write(b)
	case httpTypeRaw:
		w.Header().Set("Content-Type", contentTypeRaw)
		w.Write(b)
	case httpTypePickle:
		w.Header().Set("Content-Type", contentTypePickle)
		w.Write(b)
	case httpTypeCSV:
		w.Header().Set("Content-Type", contentTypeCSV)
		w.Write(b)
	case httpTypePNG:
		w.Header().Set("Content-Type", contentTypePNG)
		w.Write(b)
	case httpTypeMsgp:
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
