package models

import (
	"fmt"

	opentracing "github.com/opentracing/opentracing-go"
	traceLog "github.com/opentracing/opentracing-go/log"
)

type MetaTagRecordUpsert struct {
	MetaTags    []string `json:"metaTags"`
	Expressions []string `json:"expressions" binding:"Required"`
}

func (m MetaTagRecordUpsert) Trace(span opentracing.Span) {
	span.LogFields(
		traceLog.String("metaTags", fmt.Sprintf("%q", m.MetaTags)),
		traceLog.String("expressions", fmt.Sprintf("%q", m.Expressions)),
	)
}

func (m MetaTagRecordUpsert) TraceDebug(span opentracing.Span) {
}

type MetaTagRecordSwap struct {
	Records []MetaTagRecord `json:"records"`
}

type MetaTagRecord struct {
	MetaTags    []string `json:"metaTags" binding:"Required"`
	Expressions []string `json:"expressions" binding:"Required"`
}

func (m MetaTagRecordSwap) Trace(span opentracing.Span) {
	span.LogFields(
		traceLog.Uint32("recordCount", uint32(len(m.Records))),
	)
}

func (m MetaTagRecordSwap) TraceDebug(span opentracing.Span) {
}
