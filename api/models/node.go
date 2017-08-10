package models

import (
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/raintank/metrictank/cluster"
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

type GetData struct {
	Requests []Req `json:"requests" binding:"Required"`
}

func (g GetData) Trace(span opentracing.Span) {
	for _, r := range g.Requests {
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
