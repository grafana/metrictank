package models

import (
	"fmt"

	opentracing "github.com/opentracing/opentracing-go"
	traceLog "github.com/opentracing/opentracing-go/log"
)

//go:generate msgp

type MetaTagRecordUpsert struct {
	MetaTags    []string `json:"metaTags" binding:"Required"`
	Expressions []string `json:"expressions" binding:"Required"`
	Propagate   bool     `json:"propagate"`
}

func (m MetaTagRecordUpsert) Trace(span opentracing.Span) {
	span.LogFields(
		traceLog.String("metaTags", fmt.Sprintf("%q", m.MetaTags)),
		traceLog.String("expressions", fmt.Sprintf("%q", m.Expressions)),
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

type MetaTagRecordSwap struct {
	Records   []MetaTagRecord `json:"records"`
	Propagate bool            `json:"propagate"`
}

type MetaTagRecord struct {
	MetaTags    []string `json:"metaTags" binding:"Required"`
	Expressions []string `json:"expressions" binding:"Required"`
}

func (m MetaTagRecordSwap) Trace(span opentracing.Span) {
	span.LogFields(
		traceLog.String("recordCount", string(len(m.Records))),
		traceLog.Bool("propagate", m.Propagate),
	)
}

func (m MetaTagRecordSwap) TraceDebug(span opentracing.Span) {
}

type MetaTagRecordSwapResultByNode struct {
	Local       MetaTagRecordSwapResult            `json:"local"`
	PeerResults map[string]MetaTagRecordSwapResult `json:"peerResults"`
	PeerErrors  map[string]string                  `json:"peerErrors"`
}

type MetaTagRecordSwapResult struct {
	Deleted uint32 `json:"deleted"`
	Added   uint32 `json:"added"`
}

type IndexMetaTagRecordSwap struct {
	OrgId     uint32          `json:"orgId" binding:"Required"`
	Records   []MetaTagRecord `json:"records"`
	Propagate bool            `json:"propagate"`
}

func (m IndexMetaTagRecordSwap) Trace(span opentracing.Span) {
	span.SetTag("orgId", m.OrgId)
	span.LogFields(
		traceLog.String("recordCount", string(len(m.Records))),
	)
}

func (m IndexMetaTagRecordSwap) TraceDebug(span opentracing.Span) {
}
