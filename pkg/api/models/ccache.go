package models

import (
	"fmt"

	opentracing "github.com/opentracing/opentracing-go"
	traceLog "github.com/opentracing/opentracing-go/log"
)

type CCacheDelete struct {
	// patterns with name globbing
	Patterns []string `json:"patterns" form:"patterns" `
	// tag expressions to select series
	Expr      []string `json:"expr" form:"expr"`
	OrgId     uint32   `json:"orgId" form:"orgId" binding:"Required"`
	Propagate bool     `json:"propagate" form:"propagate"`
}

func (cd CCacheDelete) Trace(span opentracing.Span) {
	span.LogFields(
		traceLog.String("patterns", fmt.Sprintf("%q", cd.Patterns)),
		traceLog.Int32("org", int32(cd.OrgId)),
		traceLog.Bool("propagate", cd.Propagate),
	)
}

func (cd CCacheDelete) TraceDebug(span opentracing.Span) {
}

type CCacheDeleteResp struct {
	Errors          int                         `json:"errors"`
	FirstError      string                      `json:"firstError"`
	DeletedSeries   int                         `json:"deletedSeries"`
	DeletedArchives int                         `json:"deletedArchives"`
	Peers           map[string]CCacheDeleteResp `json:"peers"`
}

func (c *CCacheDeleteResp) AddError(err error) {
	if c.Errors == 0 {
		c.FirstError = err.Error()
	}
	c.Errors++
}
