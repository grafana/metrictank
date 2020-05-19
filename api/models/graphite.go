package models

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"

	"github.com/go-macaron/binding"
	"github.com/grafana/metrictank/idx"
	pickle "github.com/kisielk/og-rek"
	opentracing "github.com/opentracing/opentracing-go"
	traceLog "github.com/opentracing/opentracing-go/log"
	"gopkg.in/macaron.v1"
)

//go:generate msgp
//msgp:ignore FromTo
//msgp:ignore GraphiteAutoCompleteTags
//msgp:ignore GraphiteAutoCompleteTagValues
//msgp:ignore GraphiteFind
//msgp:ignore GraphiteRender
//msgp:ignore GraphiteTag
//msgp:ignore GraphiteTagDetails
//msgp:ignore GraphiteTagDetailsResp
//msgp:ignore GraphiteTagDetailsValueResp
//msgp:ignore GraphiteTagFindSeries
//msgp:ignore GraphiteTagFindSeriesResp
//msgp:ignore GraphiteTagFindSeriesLastTsResp
//msgp:ignore GraphiteTagFindSeriesMetaResp
//msgp:ignore GraphiteTagResp
//msgp:ignore GraphiteTags
//msgp:ignore GraphiteTagsResp
//msgp:ignore MetricNames
//msgp:ignore MetricsDelete
//msgp:ignore SeriesCompleter
//msgp:ignore SeriesCompleterItem
//msgp:ignore SeriesLastTs
//msgp:ignore SeriesTree
//msgp:ignore SeriesTreeItem
//msgp:ignore SeriesDetails
//msgp:ignore SeriesDetailItem
//msgp:ignore SeriesDetailItemArchive

type FromTo struct {
	From  string `json:"from" form:"from"`
	Until string `json:"until" form:"until"`
	To    string `json:"to" form:"to"` // graphite uses 'until' but we allow to alternatively cause it's shorter
	Tz    string `json:"tz" form:"tz"`
}

type GraphiteRender struct {
	FromTo
	MaxDataPoints uint32   `json:"maxDataPoints" form:"maxDataPoints" binding:"Default(800)"`
	Targets       []string `json:"target" form:"target"`
	TargetsRails  []string `form:"target[]"` // # Rails/PHP/jQuery common practice format: ?target[]=path.1&target[]=path.2 -> like graphite, we allow this.
	Format        string   `json:"format" form:"format" binding:"In(,json,msgp,msgpack,pickle)"`
	NoProxy       bool     `json:"local" form:"local"` //this is set to true by graphite-web when it passes request to cluster servers
	Meta          bool     `json:"meta" form:"meta"`   // request for meta data, which will be returned as long as the format is compatible (json) and we don't have to go via graphite
	Process       string   `json:"process" form:"process" binding:"In(,none,stable,any);Default(stable)"`
	Optimizations string   `json:"optimizations" form:"optimizations"`
}

func (gr GraphiteRender) Validate(ctx *macaron.Context, errs binding.Errors) binding.Errors {
	if len(gr.Targets) == 0 {
		if len(gr.TargetsRails) == 0 {
			errs = append(errs, binding.Error{
				FieldNames:     []string{"target"},
				Classification: "RequiredError",
				Message:        "Required",
			})
			return errs
		}
		gr.Targets = gr.TargetsRails
	}
	for _, val := range gr.Targets {
		if val == "" {
			errs = append(errs, binding.Error{
				FieldNames:     []string{"target"},
				Classification: "RequiredError",
				Message:        "Required",
			})
		}
	}
	return errs
}

type GraphiteTags struct {
	Filter string `json:"filter" form:"filter"`
}

type GraphiteTagsResp []GraphiteTagResp

type GraphiteAutoCompleteTags struct {
	Prefix string   `json:"tagPrefix" form:"tagPrefix"`
	Expr   []string `json:"expr" form:"expr"`
	Limit  uint     `json:"limit" form:"limit"`
}

type GraphiteAutoCompleteTagValues struct {
	Tag    string   `json:"tag" form:"tag"`
	Prefix string   `json:"valuePrefix" form:"valuePrefix"`
	Expr   []string `json:"expr" form:"expr"`
	Limit  uint     `json:"limit" form:"limit"`
}

type GraphiteTagResp struct {
	Tag string `json:"tag"`
}

type GraphiteTagDetails struct {
	Tag    string `json:"tag" form:"tag"`
	Filter string `json:"filter" form:"filter"`
}

type GraphiteTagDetailsResp struct {
	Tag    string                        `json:"tag"`
	Values []GraphiteTagDetailsValueResp `json:"values"`
}

type GraphiteTagDetailsValueResp struct {
	Count uint64 `json:"count"`
	Value string `json:"value"`
}

type GraphiteTagFindSeries struct {
	Expr   []string `json:"expr" form:"expr"`
	From   int64    `json:"from" form:"from"`
	Format string   `json:"format" form:"format" binding:"In(,series-json,lastts-json);Default(series-json)"`
	Limit  int      `json:"limit" binding:"Default(0)"`
	Meta   bool     `json:"meta" binding:"Default(false)"`
}

type GraphiteTagFindSeriesResp struct {
	Series []string `json:"series"`
}

type SeriesLastTs struct {
	Series string `json:"val"`
	Ts     int64  `json:"lastTs"`
}

type GraphiteTagFindSeriesLastTsResp struct {
	Series   []SeriesLastTs `json:"series"`
	Warnings []string       `json:"warnings,omitempty"`
}

type GraphiteTagFindSeriesMetaResp struct {
	Series   []string `json:"series"`
	Warnings []string `json:"warnings,omitempty"`
}

type GraphiteTagDelSeries struct {
	Paths []string `json:"path" form:"path"`
}

func (g GraphiteTagDelSeries) Trace(span opentracing.Span) {
	span.LogFields(
		traceLog.String("paths", fmt.Sprintf("%q", g.Paths)),
	)
}

func (g GraphiteTagDelSeries) TraceDebug(span opentracing.Span) {
}

type GraphiteTagDelSeriesResp struct {
	Count int            `json:"count"`
	Peers map[string]int `json:"peers"`
}

type GraphiteTagTerms struct {
	Tags []string `json:"tags"`
	Expr []string `json:"expressions"`
}

type GraphiteTagTermsResp struct {
	TotalSeries uint32                       `json:"totalSeries"`
	Terms       map[string]map[string]uint32 `json:"terms"`
}

type GraphiteFind struct {
	FromTo
	Query  string `json:"query" form:"query" binding:"Required"`
	Format string `json:"format" form:"format" binding:"In(,completer,json,treejson,msgpack,pickle,details)"`
	Jsonp  string `json:"jsonp" form:"jsonp"`
}

type MetricsDelete struct {
	Query string `json:"query" form:"query" binding:"Required"`
}

type MetricNames []idx.Archive

func (defs MetricNames) MarshalJSONFast(b []byte) ([]byte, error) {
	seen := make(map[string]struct{})

	names := make([]string, 0, len(defs))

	for i := 0; i < len(defs); i++ {
		_, ok := seen[defs[i].Name]
		if !ok {
			names = append(names, defs[i].Name)
			seen[defs[i].Name] = struct{}{}
		}
	}
	sort.Strings(names)
	b = append(b, '[')
	for _, name := range names {
		b = strconv.AppendQuoteToASCII(b, name)
		b = append(b, ',')
	}
	if len(defs) != 0 {
		b = b[:len(b)-1] // cut last comma
	}
	b = append(b, ']')
	return b, nil
}

func (defs MetricNames) MarshalJSON() ([]byte, error) {
	return defs.MarshalJSONFast(nil)
}

type SeriesCompleter map[string][]SeriesCompleterItem

func NewSeriesCompleter() SeriesCompleter {
	return SeriesCompleter(map[string][]SeriesCompleterItem{"metrics": make([]SeriesCompleterItem, 0)})
}

func (c SeriesCompleter) Add(e SeriesCompleterItem) {
	c["metrics"] = append(c["metrics"], e)
}

type SeriesCompleterItem struct {
	Path   string `json:"path"`
	Name   string `json:"name"`
	IsLeaf string `json:"is_leaf"`
}

type SeriesPickle []SeriesPickleItem

func (s SeriesPickle) Pickle(buf []byte) ([]byte, error) {
	buffer := bytes.NewBuffer(buf)
	encoder := pickle.NewEncoder(buffer)
	err := encoder.Encode(s)
	return buffer.Bytes(), err
}

type SeriesPickleItem struct {
	Path      string    `pickle:"path" msg:"path"`
	IsLeaf    bool      `pickle:"isLeaf" msg:"isLeaf"`
	Intervals [][]int64 `pickle:"intervals" msg:"intervals"` // list of (start,end) tuples
}

func NewSeriesPickleItem(path string, isLeaf bool, intervals [][]int64) SeriesPickleItem {
	return SeriesPickleItem{
		Path:      path,
		IsLeaf:    isLeaf,
		Intervals: intervals,
	}
}

type SeriesTree []SeriesTreeItem

func (s *SeriesTree) Add(i *SeriesTreeItem) {
	*s = append(*s, *i)
}

type SeriesTreeItem struct {
	AllowChildren int            `json:"allowChildren"`
	Expandable    int            `json:"expandable"`
	Leaf          int            `json:"leaf"`
	ID            string         `json:"id"`
	Text          string         `json:"text"`
	Context       map[string]int `json:"context"` // unused
}

type SeriesDetails []SeriesDetailItem

func (s *SeriesDetails) Add(i *SeriesDetailItem) {
	*s = append(*s, *i)
}

type SeriesDetailItem struct {
	AllowChildren int                       `json:"allowChildren"`
	Expandable    int                       `json:"expandable"`
	Leaf          int                       `json:"leaf"`
	ID            string                    `json:"id"`
	Text          string                    `json:"text"`
	Archives      []SeriesDetailItemArchive `json:"archives"`
}

func NewSeriesDetailItem(id, text string, allowChildren, expandable, leaf int, ar []idx.Archive) *SeriesDetailItem {
	di := SeriesDetailItem{
		ID:            id,
		Text:          text,
		AllowChildren: allowChildren,
		Expandable:    expandable,
		Leaf:          leaf,
	}
	arcs := make([]SeriesDetailItemArchive, 0, len(ar))
	for _, a := range ar {
		b := SeriesDetailItemArchive{
			MKey:         a.MetricDefinition.Id.String(),
			OrgId:        a.OrgId,
			Name:         a.Name,
			Interval:     a.Interval,
			Unit:         a.Unit,
			Mtype:        a.Mtype,
			Tags:         a.Tags,
			LastUpdate:   a.LastUpdate,
			Partition:    a.Partition,
			nameWithTags: a.NameWithTags(),

			SchemaId: a.SchemaId,
			AggId:    a.AggId,
			IrId:     a.IrId,
			LastSave: a.LastSave,
		}
		arcs = append(arcs, b)
	}
	di.Archives = arcs
	return &di
}

type SeriesDetailItemArchive struct {
	MKey         string   `json:"mkey"`
	OrgId        uint32   `json:"org_id"`
	Name         string   `json:"name"`
	Interval     int      `json:"interval"`
	Unit         string   `json:"unit"`
	Mtype        string   `json:"mtype"`
	Tags         []string `json:"tags"`
	LastUpdate   int64    `json:"lastUpdate"`
	Partition    int32    `json:"partition"`
	nameWithTags string

	SchemaId uint16 `json:"schema_id"`
	AggId    uint16 `json:"agg_id"`
	IrId     uint16 `json:"ir_id"`
	LastSave uint32 `json:"lastSave"`
}
