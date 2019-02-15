package models

import (
	opentracing "github.com/opentracing/opentracing-go"
)

type MetaTagRecordUpsert struct {
	MetaTags  []string `json:"metaTags" binding:"Required"`
	Queries   []string `json:"queries" binding:"Required"`
	Propagate bool     `json:"propagate"`
}

func (m MetaTagRecordUpsert) Trace(span opentracing.Span) {
	span.SetTag("metaTags", m.MetaTags)
	span.SetTag("queries", m.Queries)
	span.SetTag("propagate", m.Propagate)
}

func (m MetaTagRecordUpsert) TraceDebug(span opentracing.Span) {
}

type MetaTagRecordUpsertResultByNode struct {
	Local       MetaTagRecordUpsertResult
	PeerResults map[string]MetaTagRecordUpsertResult `json:"peerResults"`
	PeerErrors  map[string]string                    `json:"peerErrors`
}

//go:generate msgp
type MetaTagRecordUpsertResult struct {
	MetaTags []string `json:"metaTags"`
	Queries  []string `json:"queries"`
	ID       uint32   `json:"id"`
	Created  bool     `json:"created"`
}

type IndexMetaTagRecordUpsert struct {
	OrgId    uint32   `json:"orgId" binding:"Required"`
	MetaTags []string `json:"metaTags" binding:"Required"`
	Queries  []string `json:"queries" binding:"Required"`
}

func (m IndexMetaTagRecordUpsert) Trace(span opentracing.Span) {
	span.SetTag("org", m.OrgId)
	span.SetTag("metaTags", m.MetaTags)
	span.SetTag("queries", m.Queries)
}

func (m IndexMetaTagRecordUpsert) TraceDebug(span opentracing.Span) {
}
