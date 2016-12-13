package models

import (
	"math"
	"strconv"

	"gopkg.in/raintank/schema.v1"
)

//go:generate msgp
type Series struct {
	Target     string // will be set to the target attribute of the given request
	Datapoints []schema.Point
	Interval   uint32
}

type SeriesByTarget []Series

func (g SeriesByTarget) Len() int           { return len(g) }
func (g SeriesByTarget) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }
func (g SeriesByTarget) Less(i, j int) bool { return g[i].Target < g[j].Target }

// regular graphite output
func (series SeriesByTarget) MarshalJSONFast(b []byte) ([]byte, error) {
	b = append(b, '[')
	for _, s := range series {
		b = append(b, `{"target":`...)
		b = strconv.AppendQuoteToASCII(b, s.Target)
		b = append(b, `,"datapoints":[`...)
		for _, p := range s.Datapoints {
			b = append(b, '[')
			if math.IsNaN(p.Val) {
				b = append(b, `null,`...)
			} else {
				b = strconv.AppendFloat(b, p.Val, 'f', 3, 64)
				b = append(b, ',')
			}
			b = strconv.AppendUint(b, uint64(p.Ts), 10)
			b = append(b, `],`...)
		}
		if len(s.Datapoints) != 0 {
			b = b[:len(b)-1] // cut last comma
		}
		b = append(b, `]},`...)
	}
	if len(series) != 0 {
		b = b[:len(b)-1] // cut last comma
	}
	b = append(b, ']')
	return b, nil
}

func (series SeriesByTarget) MarshalJSON() ([]byte, error) {
	return series.MarshalJSONFast(nil)
}
