package models

import (
	"github.com/grafana/metrictank/cluster"
	opentracing "github.com/opentracing/opentracing-go"
)

type NodeStatus struct {
	Primary string `json:"primary" form:"primary" binding:"Required"`
}

type ClusterStatus struct {
	ClusterName string         `json:"clusterName"`
	NodeName    string         `json:"nodeName"`
	Members     []cluster.Node `json:"members"`
}

type ClusterMembers struct {
	Members []string `json:"members"`
}

type ClusterMembersResp struct {
	Status       string `json:"status"`
	MembersAdded int    `json:"membersAdded"`
}

type IndexList struct {
	OrgId int `json:"orgId" form:"orgId" binding:"Required"`
}

func (i IndexList) Trace(span opentracing.Span) {
	span.SetTag("org", i.OrgId)
}

func (i IndexList) TraceDebug(span opentracing.Span) {
}

type IndexFindByTag struct {
	OrgId int      `json:"orgId" binding:"Required"`
	Expr  []string `json:"expressions"`
	From  int64    `json:"from"`
}

func (t IndexFindByTag) Trace(span opentracing.Span) {
	span.SetTag("org", t.OrgId)
	span.SetTag("expressions", t.Expr)
	span.SetTag("from", t.From)
}

func (i IndexFindByTag) TraceDebug(span opentracing.Span) {
}

type IndexTagDetails struct {
	OrgId  int    `json:"orgId" binding:"Required"`
	Filter string `json:"filter"`
	Tag    string `json:"tag" binding:"Required"`
	From   int64  `json:"from"`
}

func (t IndexTagDetails) Trace(span opentracing.Span) {
	span.SetTag("org", t.OrgId)
	span.SetTag("filter", t.Filter)
	span.SetTag("tag", t.Tag)
	span.SetTag("from", t.From)
}

func (i IndexTagDetails) TraceDebug(span opentracing.Span) {
}

type IndexTags struct {
	OrgId  int    `json:"orgId" binding:"Required"`
	Filter string `json:"filter"`
	From   int64  `json:"from"`
}

func (t IndexTags) Trace(span opentracing.Span) {
	span.SetTag("org", t.OrgId)
	span.SetTag("filter", t.Filter)
	span.SetTag("from", t.From)
}

func (i IndexTags) TraceDebug(span opentracing.Span) {
}

type IndexAutoCompleteTags struct {
	OrgId     int      `json:"orgId" binding:"Required"`
	TagPrefix string   `json:"tagPrefix"`
	Expr      []string `json:"expressions"`
	From      int64    `json:"from"`
	Limit     uint     `json:"limit"`
}

func (t IndexAutoCompleteTags) Trace(span opentracing.Span) {
	span.SetTag("org", t.OrgId)
	span.SetTag("tagPrefix", t.TagPrefix)
	span.SetTag("expressions", t.Expr)
	span.SetTag("from", t.From)
	span.SetTag("limit", t.Limit)
}

func (i IndexAutoCompleteTags) TraceDebug(span opentracing.Span) {
}

type IndexAutoCompleteTagValues struct {
	OrgId     int      `json:"orgId" binding:"Required"`
	ValPrefix string   `json:"valPrefix"`
	Tag       string   `json:"tag"`
	Expr      []string `json:"expressions"`
	From      int64    `json:"from"`
	Limit     uint     `json:"limit"`
}

func (t IndexAutoCompleteTagValues) Trace(span opentracing.Span) {
	span.SetTag("org", t.OrgId)
	span.SetTag("valPrefix", t.ValPrefix)
	span.SetTag("tag", t.Tag)
	span.SetTag("expressions", t.Expr)
	span.SetTag("from", t.From)
	span.SetTag("limit", t.Limit)
}

func (i IndexAutoCompleteTagValues) TraceDebug(span opentracing.Span) {
}

type IndexGet struct {
	Id string `json:"id" form:"id" binding:"Required"`
}

type IndexFind struct {
	Patterns []string `json:"patterns" form:"patterns" binding:"Required"`
	OrgId    int      `json:"orgId" form:"orgId" binding:"Required"`
	From     int64    `json:"from" form:"from"`
}

func (i IndexFind) Trace(span opentracing.Span) {
	span.SetTag("q", i.Patterns)
	span.SetTag("org", i.OrgId)
	span.SetTag("from", i.From)
}

func (i IndexFind) TraceDebug(span opentracing.Span) {
}

type GetData struct {
	Requests []Req `json:"requests" binding:"Required"`
}

func (g GetData) Trace(span opentracing.Span) {
	span.SetTag("num_reqs", len(g.Requests))
}

func (g GetData) TraceDebug(span opentracing.Span) {
	// max span size is 64kB. anything higher will be discarded
	tolog := g.Requests
	if len(g.Requests) > 45 {
		span.SetTag("udp_cutoff", true)
		tolog = g.Requests[:45]
	}
	for _, r := range tolog {
		r.TraceLog(span)
	}
}

type IndexDelete struct {
	Query string `json:"query" form:"query" binding:"Required"`
	OrgId int    `json:"orgId" form:"orgId" binding:"Required"`
}

func (i IndexDelete) Trace(span opentracing.Span) {
	span.SetTag("q", i.Query)
	span.SetTag("org", i.OrgId)
}

func (i IndexDelete) TraceDebug(span opentracing.Span) {
}
