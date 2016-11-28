package models

import (
	"sort"
	"strconv"

	"github.com/go-macaron/binding"
	"gopkg.in/macaron.v1"
	"gopkg.in/raintank/schema.v1"
)

type GraphiteRender struct {
	MaxDataPoints uint32   `json:"maxDataPoints" form:"maxDataPoints" binding:"Default(800)"`
	Targets       []string `json:"target" form:"target" binding:"Required"`
	From          string   `json:"from" form:"from"`
	Until         string   `json:"until" form:"until"`
	To            string   `json:"to" form:"to"`
	Format        string   `json:"format" form:"format" binding:"In(,json)"`
}

func (gr GraphiteRender) Validate(ctx *macaron.Context, errs binding.Errors) binding.Errors {
	if len(gr.Targets) < 1 {
		errs = append(errs, binding.Error{
			FieldNames:     []string{"target"},
			Classification: "RequiredError",
			Message:        "Required",
		})
	} else {
		for _, val := range gr.Targets {
			if val == "" {
				errs = append(errs, binding.Error{
					FieldNames:     []string{"target"},
					Classification: "RequiredError",
					Message:        "Required",
				})
			}
		}
	}
	return errs
}

type GraphiteFind struct {
	Query  string `json:"query" form:"query" binding:"Required"`
	From   int64  `json:"from" form:"from"`
	Until  int64  `json:"until" form:"until"`
	Format string `json:"format" form:"format" binding:"In(,completer,json,treejson)"`
	Jsonp  string `json:"jsonp" form:"jsonp"`
}

type MetricsDelete struct {
	Query string `json:"query" form:"query" binding:"Required"`
}

type MetricNames []schema.MetricDefinition

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
