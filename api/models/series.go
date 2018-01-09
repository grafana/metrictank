package models

import (
	"bytes"
	"math"
	"strconv"
	"strings"

	"github.com/grafana/metrictank/consolidation"
	pickle "github.com/kisielk/og-rek"
	"gopkg.in/raintank/schema.v1"
)

//go:generate msgp

type Series struct {
	Target       string // for fetched data, set from models.Req.Target, i.e. the metric graphite key. for function output, whatever should be shown as target string (legend)
	Datapoints   []schema.Point
	Tags         map[string]string // Must be set initially via call to `SetTags()`
	Interval     uint32
	QueryPatt    string                     // to tie series back to request it came from. e.g. foo.bar.*, or if series outputted by func it would be e.g. scale(foo.bar.*,0.123456)
	QueryFrom    uint32                     // to tie series back to request it came from
	QueryTo      uint32                     // to tie series back to request it came from
	QueryCons    consolidation.Consolidator // to tie series back to request it came from (may be 0 to mean use configured default)
	Consolidator consolidation.Consolidator // consolidator to actually use (for fetched series this may not be 0, default must be resolved. if series created by function, may be 0)
}

func (s *Series) SetTags() {
	tagSplits := strings.Split(s.Target, ";")

	s.Tags = make(map[string]string, len(tagSplits))

	for _, tagPair := range tagSplits[1:] {
		parts := strings.SplitN(tagPair, "=", 2)
		if len(parts) != 2 {
			// Shouldn't happen
			continue
		}
		s.Tags[parts[0]] = parts[1]
	}
	s.Tags["name"] = tagSplits[0]
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
		if len(s.Tags) != 0 {
			b = append(b, `,"tags":{`...)
			for name, value := range s.Tags {
				b = strconv.AppendQuoteToASCII(b, name)
				b = append(b, ':')
				b = strconv.AppendQuoteToASCII(b, value)
				b = append(b, ',')
			}
			// Replace trailing comma with a closing bracket
			b[len(b)-1] = '}'
		}
		b = append(b, `,"datapoints":[`...)
		for _, p := range s.Datapoints {
			b = append(b, '[')
			if math.IsNaN(p.Val) {
				b = append(b, `null,`...)
			} else {
				b = strconv.AppendFloat(b, p.Val, 'f', -1, 64)
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

func (series SeriesByTarget) ForGraphite(format string) SeriesListForPickle {
	var none interface{}
	if format == "pickle" {
		none = pickle.None{}
	}
	data := make(SeriesListForPickle, len(series))
	for i, s := range series {
		datapoints := make([]interface{}, len(s.Datapoints))
		for j, p := range s.Datapoints {
			if math.IsNaN(p.Val) {
				datapoints[j] = none
			} else {
				datapoints[j] = p.Val
			}
		}
		data[i] = SeriesForPickle{
			Name:           s.Target,
			Step:           s.Interval,
			Values:         datapoints,
			PathExpression: s.QueryPatt,
		}
		if len(datapoints) > 0 {
			data[i].Start = s.Datapoints[0].Ts
			data[i].End = s.Datapoints[len(s.Datapoints)-1].Ts + s.Interval
		} else {
			data[i].Start = s.QueryFrom
			data[i].End = s.QueryTo
		}
	}
	return data
}

func (series SeriesByTarget) Pickle(buf []byte) ([]byte, error) {
	buffer := bytes.NewBuffer(buf)
	encoder := pickle.NewEncoder(buffer)
	err := encoder.Encode(series.ForGraphite("pickle"))
	return buffer.Bytes(), err
}

type SeriesListForPickle []SeriesForPickle

type SeriesForPickle struct {
	Name           string        `pickle:"name" msg:"name"`
	Start          uint32        `pickle:"start" msg:"start"`
	End            uint32        `pickle:"end" msg:"end"`
	Step           uint32        `pickle:"step" msg:"step"`
	Values         []interface{} `pickle:"values" msg:"values"`
	PathExpression string        `pickle:"pathExpression" msg:"pathExpression"`
}
