package main

import (
	"github.com/dgryski/go-tsz"
)

type Iter struct {
	*tsz.Iter
	cass bool //true = cass, false = mem
}

func NewIter(i *tsz.Iter, cass bool) Iter {
	return Iter{
		i,
		cass,
	}
}
