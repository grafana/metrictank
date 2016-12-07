package iter

import (
	"github.com/dgryski/go-tsz"
)

type Iter struct {
	*tsz.Iter
}

func New(i *tsz.Iter) Iter {
	return Iter{
		i,
	}
}
