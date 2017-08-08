package models

import (
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

type IndexGet struct {
	Id string `json:"id" form:"id" binding:"Required"`
}

type IndexFind struct {
	Patterns []string `json:"patterns" form:"patterns" binding:"Required"`
	OrgId    int      `json:"orgId" form:"orgId" binding:"Required"`
	From     int64    `json:"from" form:"from"`
}

type GetData struct {
	Requests []Req `json:"requests" binding:"Required"`
}

type IndexDelete struct {
	Query string `json:"query" form:"query" binding:"Required"`
	OrgId int    `json:"orgId" form:"orgId" binding:"Required"`
}
