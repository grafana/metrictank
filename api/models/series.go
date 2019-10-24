package models

import (
	"bytes"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/schema"
	pickle "github.com/kisielk/og-rek"
)

//go:generate msgp
//msgp:ignore SeriesMetaPropertiesExport

type Series struct {
	Target       string            // for fetched data, set from models.Req.Target, i.e. the metric graphite key. for function output, whatever should be shown as target string (legend)
	Tags         map[string]string // Must be set initially via call to `SetTags()`
	Interval     uint32
	QueryPatt    string                     // to tie series back to request it came from. e.g. foo.bar.*, or if series outputted by func it would be e.g. scale(foo.bar.*,0.123456)
	QueryFrom    uint32                     // to tie series back to request it came from
	QueryTo      uint32                     // to tie series back to request it came from
	QueryCons    consolidation.Consolidator // to tie series back to request it came from (may be 0 to mean use configured default)
	Consolidator consolidation.Consolidator // consolidator to actually use (for fetched series this may not be 0, default must be resolved. if series created by function, may be 0)
	Meta         SeriesMeta                 // note: this series could be a "just fetched" series, or one derived from many other series
	Datapoints   []schema.Point
}

// SeriesMeta counts the number of series for each set of meta properties
// note: it's illegal for SeriesMeta to include multiple entries that include the same properties
type SeriesMeta []SeriesMetaProperties

// SeriesMetaProperties describes the properties of a series
// (fetching and normalization and the count of series corresponding to them)
type SeriesMetaProperties struct {
	SchemaID              uint16                     // id of storage-schemas rule this series corresponds to
	Archive               uint8                      // which archive was being read from
	AggNumNorm            uint32                     // aggNum for normalization
	AggNumRC              uint32                     // aggNum runtime consolidation
	ConsolidatorNormFetch consolidation.Consolidator // consolidator used for normalization and reading from store (if applicable)
	ConsolidatorRC        consolidation.Consolidator // consolidator used for runtime consolidation to honor maxdatapoints (if applicable).
	Count                 uint32                     // number of series corresponding to these properties
}

// SeriesMetaPropertiesExport is an "export" of a SeriesMetaProperties
// it is a more user friendly representation
type SeriesMetaPropertiesExport struct {
	SchemaName            string                     // name of schema rule used
	SchemaRetentions      string                     // schema retentions used
	ArchiveRead           uint8                      // which archive was being read from
	AggNumNorm            uint32                     // aggNum for normalization
	AggNumRC              uint32                     // aggNum runtime consolidation
	ConsolidatorNormFetch consolidation.Consolidator // consolidator used for normalization and reading from store (if applicable)
	ConsolidatorRC        consolidation.Consolidator // consolidator used for runtime consolidation to honor maxdatapoints (if applicable).
	Count                 uint32                     // number of series corresponding to these properties
}

// Export returns a human-friendly version of the SeriesMetaProperties.
func (smp SeriesMetaProperties) Export() SeriesMetaPropertiesExport {
	schema := mdata.Schemas.Get(smp.SchemaID)
	return SeriesMetaPropertiesExport{
		SchemaName:            schema.Name,
		SchemaRetentions:      schema.Retentions.Orig,
		ArchiveRead:           smp.Archive,
		AggNumNorm:            smp.AggNumNorm,
		AggNumRC:              smp.AggNumRC,
		ConsolidatorNormFetch: smp.ConsolidatorNormFetch,
		ConsolidatorRC:        smp.ConsolidatorRC,
		Count:                 smp.Count,
	}
}

// Merge merges SeriesMeta b into a
// counts for identical properties get added together
func (a SeriesMeta) Merge(b SeriesMeta) SeriesMeta {
	// note: to see which properties are equivalent we should not consider the count
	indices := make(map[SeriesMetaProperties]int)
	for i, v := range a {
		v.Count = 0
		indices[v] = i
	}
	for j, v := range b {
		v.Count = 0
		index, ok := indices[v]
		if ok {
			a[index].Count += b[j].Count
		} else {
			a = append(a, b[j])
		}
	}
	return a
}

// Copy creates a copy of SeriesMeta
func (a SeriesMeta) Copy() SeriesMeta {
	out := make(SeriesMeta, len(a))
	copy(out, a)
	return out
}

// CopyWithChange creates a copy of SeriesMeta, but executes the requested change on each SeriesMetaProperties
func (a SeriesMeta) CopyWithChange(fn func(in SeriesMetaProperties) SeriesMetaProperties) SeriesMeta {
	out := make(SeriesMeta, len(a))
	for i, v := range a {
		out[i] = fn(v)
	}
	return out
}

func (s *Series) SetTags() {
	numTags := strings.Count(s.Target, ";")

	if s.Tags == nil {
		// +1 for the name tag
		s.Tags = make(map[string]string, numTags+1)
	} else {
		for k := range s.Tags {
			delete(s.Tags, k)
		}
	}

	if numTags == 0 {
		s.Tags["name"] = s.Target
		return
	}

	index := strings.IndexByte(s.Target, ';')
	name := s.Target[:index]

	remainder := s.Target
	for index > 0 {
		remainder = remainder[index+1:]
		index = strings.IndexByte(remainder, ';')

		tagPair := remainder
		if index > 0 {
			tagPair = remainder[:index]
		}

		equalsPos := strings.IndexByte(tagPair, '=')
		if equalsPos < 1 {
			// Shouldn't happen
			continue
		}

		s.Tags[tagPair[:equalsPos]] = tagPair[equalsPos+1:]
	}

	// Do this last to overwrite any "name" tag that might have been specified in the series tags.
	s.Tags["name"] = name
}

func (s *Series) EnrichWithTags(tags tagquery.Tags) {
	// every serie has at least one tag, the name tag
	// if there are no tags defined, this means the tags must not have been set yet
	if len(s.Tags) == 0 {
		s.SetTags()
	}

	for _, tag := range tags {
		if _, ok := s.Tags[tag.Key]; !ok {
			s.Tags[tag.Key] = tag.Value
		}
	}

	s.buildTargetFromTags()
}

func (s *Series) buildTargetFromTags() {
	buf := bytes.NewBufferString(s.Tags["name"])

	keys := make([]string, 0, len(s.Tags)-1)
	for key := range s.Tags {
		if key == "name" {
			continue
		}
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key := range keys {
		buf.WriteRune(';')
		buf.WriteString(key)
		buf.WriteRune('=')
		buf.WriteString(s.Tags[key])
	}

	s.Target = buf.String()
}

// Copy returns a deep copy.
// The returned value does not link to the same memory space for any of the properties
func (s Series) Copy(emptyDatapoints []schema.Point) Series {
	return Series{
		Target:       s.Target,
		Datapoints:   append(emptyDatapoints, s.Datapoints...),
		Tags:         s.CopyTags(),
		Interval:     s.Interval,
		QueryPatt:    s.QueryPatt,
		QueryFrom:    s.QueryFrom,
		QueryTo:      s.QueryTo,
		QueryCons:    s.QueryCons,
		Consolidator: s.Consolidator,
		Meta:         s.Meta.Copy(),
	}
}

// CopyTags makes a deep copy of the tags
func (s *Series) CopyTags() map[string]string {
	out := make(map[string]string, len(s.Tags))
	for k, v := range s.Tags {
		out[k] = v
	}
	return out
}

// CopyTagsWith makes a deep copy of the tags and sets the given tag
func (s *Series) CopyTagsWith(key, val string) map[string]string {
	out := make(map[string]string, len(s.Tags)+1)
	for k, v := range s.Tags {
		out[k] = v
	}
	out[key] = val
	return out
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
func (series SeriesByTarget) MarshalJSONFastWithMeta(b []byte) ([]byte, error) {
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
		b = append(b, `],"meta":`...)
		b, _ = s.Meta.MarshalJSONFast(b)
		b = append(b, `},`...)
	}
	if len(series) != 0 {
		b = b[:len(b)-1] // cut last comma
	}
	b = append(b, ']')
	return b, nil
}

func (meta SeriesMeta) MarshalJSONFast(b []byte) ([]byte, error) {
	b = append(b, '[')
	for _, props := range meta {
		exp := props.Export()
		b = append(b, `{"schema-name":"`...)
		b = append(b, exp.SchemaName...)
		b = append(b, `","schema-retentions":"`...)
		b = append(b, exp.SchemaRetentions...)
		b = append(b, `","archive-read":`...)
		b = strconv.AppendUint(b, uint64(exp.ArchiveRead), 10)
		b = append(b, `,"aggnum-norm":`...)
		b = strconv.AppendUint(b, uint64(exp.AggNumNorm), 10)
		b = append(b, `,"consolidate-normfetch":"`...)
		b = append(b, exp.ConsolidatorNormFetch.String()...)
		b = append(b, `","aggnum-rc":`...)
		b = strconv.AppendUint(b, uint64(exp.AggNumRC), 10)
		b = append(b, `,"consolidate-rc":"`...)
		b = append(b, exp.ConsolidatorRC.String()...)
		b = append(b, `","count":`...)
		b = strconv.AppendUint(b, uint64(exp.Count), 10)
		b = append(b, `},`...)
	}
	if len(meta) != 0 {
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
