package out

import (
	"errors"

	"github.com/grafana/metrictank/pkg/schema"
	"github.com/raintank/dur"
)

type OffsetFilter struct {
	out    Out
	offset int64
}

func NewOffsetFilter(out Out, opts string) (OffsetFilter, error) {

	f := OffsetFilter{
		out: out,
	}

	if len(opts) == 0 {
		return f, errors.New("no offset specified")
	}

	sign := int64(1)

	if opts[0] == '+' {
		opts = opts[1:]
	}
	if opts[0] == '-' {
		sign = -1
		opts = opts[1:]
	}
	offset, err := dur.ParseDuration(opts)
	if err != nil {
		return f, err
	}

	f.offset = int64(offset) * sign
	return f, nil
}

func (of OffsetFilter) Close() error {
	return of.out.Close()
}

func (of OffsetFilter) Flush(metrics []*schema.MetricData) error {
	for i := range metrics {
		metrics[i].Time += of.offset
	}
	return of.out.Flush(metrics)
}
