package models

import (
	"fmt"

	opentracing "github.com/opentracing/opentracing-go"
	traceLog "github.com/opentracing/opentracing-go/log"
)

type MetaTagRecordUpsert struct {
	MetaTags  []string `json:"metaTags" binding:"Required"`
	Queries   []string `json:"queries" binding:"Required"`
	Propagate bool     `json:"propagate"`
}

func (m MetaTagRecordUpsert) Trace(span opentracing.Span) {
	span.LogFields(
		traceLog.String("metaTags", fmt.Sprintf("%q", m.MetaTags)),
		traceLog.String("queries", fmt.Sprintf("%q", m.Queries)),
		traceLog.Bool("propagate", m.Propagate),
	)
}

func (m MetaTagRecordUpsert) TraceDebug(span opentracing.Span) {
}

type MetaTagRecordUpsertResultByNode struct {
	Local       MetaTagRecordUpsertResult
	PeerResults map[string]MetaTagRecordUpsertResult `json:"peerResults"`
	PeerErrors  map[string]string                    `json:"peerErrors"`
}

//go:generate msgp
type MetaTagRecordUpsertResult struct {
	MetaTags []string `json:"metaTags"`
	Queries  []string `json:"queries"`
	Created  bool     `json:"created"`
}

type IndexMetaTagRecordUpsert struct {
	OrgId    uint32   `json:"orgId" binding:"Required"`
	MetaTags []string `json:"metaTags" binding:"Required"`
	Queries  []string `json:"queries" binding:"Required"`
}

func (m IndexMetaTagRecordUpsert) Trace(span opentracing.Span) {
	span.SetTag("orgId", m.OrgId)
	span.LogFields(
		traceLog.String("metaTags", fmt.Sprintf("%q", m.MetaTags)),
		traceLog.String("queries", fmt.Sprintf("%q", m.Queries)),
	)
}

func (m IndexMetaTagRecordUpsert) TraceDebug(span opentracing.Span) {
}
