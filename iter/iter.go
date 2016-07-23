package iter

import (
	"github.com/dgryski/go-tsz"
)

// Iter is a simple wrapper around a tsz.Iter to make debug logging easier
type Iter struct {
	*tsz.Iter
	Cass bool //true = cass, false = mem
}

func New(i *tsz.Iter, cass bool) Iter {
	return Iter{
		i,
		cass,
	}
}
