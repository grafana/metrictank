package main

import (
	"fmt"
	"github.com/dgryski/go-tsz"
)

type Iter struct {
	cmt string // anything descriptive
	i   *tsz.Iter
}

func NewIter(i *tsz.Iter, format string, a ...interface{}) Iter {
	return Iter{
		cmt: fmt.Sprintf(format, a...),
		i:   i,
	}
}

func (i Iter) Next() bool {
	return i.i.Next()
}
func (i Iter) Values() (uint32, float64) {
	return i.i.Values()
}

func (i Iter) Err() error {
	return i.i.Err()
}
