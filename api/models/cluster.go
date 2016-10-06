package models

type NodeStatus struct {
	Primary bool `json:"primary" form:"primary" binding:"required"`
}
