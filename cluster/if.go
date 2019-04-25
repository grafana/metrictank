package cluster

import (
	"context"
)

type Node interface {
	IsLocal() bool
	IsReady() bool
	GetPartitions() []int32
	GetPriority() int
	HasData() bool
	Post(context.Context, string, string, Traceable) ([]byte, error)
	GetName() string
}
