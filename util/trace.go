package util

import (
	"context"
)

type TraceID struct{}

func ExtractTraceID(ctx context.Context) string {
	traceID := ctx.Value(TraceID{})
	if traceID == nil {
		return ""
	}

	return traceID.(string)
}
