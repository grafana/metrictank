package models

type NormalGCPercent struct {
	Value int `json:"value" form:"value" binding:"Required"`
}

type StartupGCPercent struct {
	Value int `json:"value" form:"value" binding:"Required"`
}
