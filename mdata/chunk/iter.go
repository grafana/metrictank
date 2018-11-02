package chunk

import (
	"github.com/grafana/metrictank/mdata/chunk/tsz"
)

type Iter struct {
	*tsz.Iter4h
}

func NewIter(i *tsz.Iter4h) Iter {
	return Iter{
		i,
	}
}
