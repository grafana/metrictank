package chunk

import (
	"github.com/grafana/metrictank/mdata/chunk/tsz"
)

type Iter struct {
	*tsz.Iter
}

func NewIter(i *tsz.Iter) Iter {
	return Iter{
		i,
	}
}
