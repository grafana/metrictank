// Copyright 2013 Martini Authors
// Copyright 2015 The Macaron Authors
// Copyright 2017 Grafana Labs
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package gziper

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/klauspost/compress/gzip"
	"gopkg.in/macaron.v1"
)

const (
	_HEADER_ACCEPT_ENCODING  = "Accept-Encoding"
	_HEADER_CONTENT_ENCODING = "Content-Encoding"
	_HEADER_CONTENT_LENGTH   = "Content-Length"
	_HEADER_CONTENT_TYPE     = "Content-Type"
	_HEADER_VARY             = "Vary"
)

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }

/*
  Gzip middleware inspired by https://github.com/go-macaron/gzip

  The main difference is that this middleware will only compress
  the body if it is not already compressed (content-type header
  already set to gzip).

  This usecase is needed when requests are conditionally proxied
  and the proxy response may or may not already be compressed.
*/
func Gziper() macaron.Handler {
	return func(ctx *macaron.Context) {
		if !strings.Contains(ctx.Req.Header.Get(_HEADER_ACCEPT_ENCODING), "gzip") {
			return
		}
		grw := &gzipResponseWriter{
			ResponseWriter: ctx.Resp,
		}
		defer grw.close()

		ctx.Resp = grw
		ctx.MapTo(grw, (*http.ResponseWriter)(nil))

		// Check if render middleware has been registered,
		// if yes, we need to modify ResponseWriter for it as well.
		if _, ok := ctx.Render.(*macaron.DummyRender); !ok {
			ctx.Render.SetResponseWriter(grw)
		}
		ctx.Next()
		// delete content length after we know we have been written to
		grw.Header().Del(_HEADER_CONTENT_LENGTH)
	}
}

type gzipResponseWriter struct {
	w io.WriteCloser
	macaron.ResponseWriter
	closed bool
}

func (grw *gzipResponseWriter) setup() {
	if grw.w != nil {
		return
	}

	if grw.closed || grw.Header().Get(_HEADER_CONTENT_ENCODING) == "gzip" {
		// another handler has already set the content-type to gzip,
		// so presumably they are going to write compressed data.
		// We dont want to compress the data a second time, so lets
		// have writes go directly to the normal ResponseWriter
		grw.w = nopCloser{grw.ResponseWriter}
	} else {
		headers := grw.Header()
		headers.Set(_HEADER_CONTENT_ENCODING, "gzip")
		headers.Set(_HEADER_VARY, _HEADER_ACCEPT_ENCODING)
		grw.w = gzip.NewWriter(grw.ResponseWriter)
	}
}

func (grw *gzipResponseWriter) close() {
	grw.closed = true
	if grw.w == nil {
		return
	}
	grw.w.Close()
}

func (grw *gzipResponseWriter) Write(p []byte) (int, error) {
	if !grw.Written() {
		// try and detect the content type.  If we dont do this here,
		// http.ResponseWriter will try and detect the content-type but
		// will be looking at the compressed payload.
		if len(grw.Header().Get(_HEADER_CONTENT_TYPE)) == 0 {
			grw.Header().Set(_HEADER_CONTENT_TYPE, http.DetectContentType(p))
		}
		grw.WriteHeader(http.StatusOK)
	}
	return grw.w.Write(p)
}

func (grw *gzipResponseWriter) WriteHeader(s int) {
	if !grw.Written() {
		grw.setup()
	}
	grw.ResponseWriter.WriteHeader(s)
}

func (grw gzipResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := grw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("the ResponseWriter doesn't support the Hijacker interface")
	}
	return hijacker.Hijack()
}
