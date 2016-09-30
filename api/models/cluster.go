package models

type ClusterStatus struct {
	Primary bool `json:"primary" form:"primary" binding:"required"`
}
