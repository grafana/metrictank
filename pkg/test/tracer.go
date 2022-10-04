package test

import (
	"context"

	opentracing "github.com/opentracing/opentracing-go"
)

// NewContext returns a context holding a tracing span
func NewContext() context.Context {
	tracer := opentracing.NoopTracer{}
	span := tracer.StartSpan("test")
	return opentracing.ContextWithSpan(context.Background(), span)
}
