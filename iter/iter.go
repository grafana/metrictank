package iter

import (
	"github.com/dgryski/go-tsz"
)

// Iter is a simple wrapper around a tsz.Iter to make debug logging easier
type Iter struct {
	*tsz.Iter
	Length int
}

func New(i *tsz.Iter, l int) Iter {
	return Iter{
		i,
		l,
	}
}
