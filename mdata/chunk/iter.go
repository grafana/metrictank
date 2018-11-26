package chunk

import (
	"github.com/dgryski/go-tsz"
)

type Iter struct {
	*tsz.Iter
}

func NewIter(i *tsz.Iter) Iter {
	return Iter{
		i,
	}
}
