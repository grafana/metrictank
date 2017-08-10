package cluster

import opentracing "github.com/opentracing/opentracing-go"

type Traceable interface {
	Trace(span opentracing.Span)
}
