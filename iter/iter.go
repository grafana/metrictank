package iter

import (
	"github.com/dgryski/go-tsz"
)

// Iter is a simple wrapper around a tsz.Iter to make debug logging easier
type Iter struct {
	*tsz.Iter
}

func New(i *tsz.Iter) Iter {
	return Iter{
		i,
	}
}
