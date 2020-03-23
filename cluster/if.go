package cluster

import (
	"context"
	"io"
)

type Node interface {
	IsLocal() bool
	IsReady() bool
	GetPartitions() []int32
	GetPriority() int
	HasData() bool
	Post(context.Context, string, string, Traceable) ([]byte, error)
	PostRaw(ctx context.Context, name, path string, body Traceable) (io.ReadCloser, error)
	GetName() string
}
