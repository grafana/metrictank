package models

import (
	"bytes"
	"sort"
	"strconv"

	"github.com/go-macaron/binding"
	pickle "github.com/kisielk/og-rek"
	"github.com/raintank/metrictank/idx"
	"gopkg.in/macaron.v1"
)

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
	Format        string   `json:"format" form:"format" binding:"In(,json,msgp,pickle)"`
	NoProxy       bool     `json:"local" form:"local"` //this is set to true by graphite-web when it passes request to cluster servers
	Process       string   `json:"process" form:"process" binding:"In(,none,stable,any);Default(stable)"`
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

type GraphiteFind struct {
	FromTo
	Query  string `json:"query" form:"query" binding:"Required"`
	Format string `json:"format" form:"format" binding:"In(,completer,json,treejson,pickle)"`
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
	Path      string    `pickle:"path"`
	IsLeaf    bool      `pickle:"isLeaf"`
	Intervals [][]int64 `pickle:"intervals"` // list of (start,end) tuples
}

func NewSeriesPickleItem(path string, isLeaf bool, intervals [][]int64) SeriesPickleItem {
	return SeriesPickleItem{
		Path:      path,
		IsLeaf:    isLeaf,
		Intervals: intervals,
	}
}

type SeriesTree []SeriesTreeItem

func NewSeriesTree() *SeriesTree {
	return new(SeriesTree)
}

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
