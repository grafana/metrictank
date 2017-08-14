package tracing

import (
	"context"
	"fmt"

	opentracing "github.com/opentracing/opentracing-go"
	tags "github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
)

// NewSpan pulls the span out of the context, creates a new child span, and updates the context
// callers must call span.Finish() when done
// important: the context must contain a span, otherwise you'll see a nilpointer panic here.
func NewSpan(ctx context.Context, tracer opentracing.Tracer, name string) (context.Context, opentracing.Span) {
	span := opentracing.SpanFromContext(ctx)
	spanCtx := span.Context()
	span = tracer.StartSpan(name, opentracing.ChildOf(spanCtx))
	ctx = opentracing.ContextWithSpan(ctx, span)
	return ctx, span
}

// Error marks the span (and parents) as failed, and logs error
func Error(span opentracing.Span, err error) {
	tags.Error.Set(span, true)
	span.LogFields(log.Error(err))
}

// Errorf marks the span (and parents) as failed, and logs error
func Errorf(span opentracing.Span, format string, a ...interface{}) {
	tags.Error.Set(span, true)
	span.LogFields(log.Error(fmt.Errorf(format, a...)))
}
