package memory

import (
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/schema"
	log "github.com/sirupsen/logrus"
)

type expressionFilter struct {
	// expr is the expression based on which this filter has been generated
	expr tagquery.Expression

	// test is a filter function which takes certain descriptive properties of a
	// MetricDefinition and returns a tagquery.FilterDecision type indicating
	// whether the MD satisfies this expression or not
	// this decision is made based on the metric tag index, the meta tag index
	// does not get take into account
	testByMetricTags tagquery.MetricDefinitionFilter

	// testByMetaTags is a filter function which has been generated from the meta
	// records that match this filter's expression.
	// it looks at certain descriptive properties of a MetricDefinition and decides
	// whether this metric definition satisfies the given expression based on the
	// meta tag index, it does not take the metric tag index into account
	testByMetaTags tagquery.MetricDefinitionFilter

	// the default decision which should be applied if none of test & testByMetaTags
	// have come to a conclusive decision
	defaultDecision tagquery.FilterDecision
}

// idFilter contains one or many filter functions which are used to filter metrics
// by the expressions from which its filters have been generated.
// once initialized it can be used concurrently by many worker routines
type idFilter struct {
	ctx     *TagQueryContext
	filters []expressionFilter
}

// newIdFilter takes a set of expressions and a tag query context, then it generates
// various filter functions from the expressions which are going to be used to decide
// whether a given metric matches the provided expressions
func newIdFilter(expressions tagquery.Expressions, ctx *TagQueryContext) *idFilter {
	res := idFilter{
		ctx:     ctx,
		filters: make([]expressionFilter, len(expressions)),
	}

	for i, expr := range expressions {
		res.filters[i] = expressionFilter{
			expr:             expr,
			testByMetricTags: expr.GetMetricDefinitionFilter(ctx.index.idHasTag),
			defaultDecision:  expr.GetDefaultDecision(),
		}

		if !MetaTagSupport {
			continue
		}

		// this is a performacnce optimization:
		// some expressions indicate that they'll likely result in a smaller result set
		// if they get inverted.
		// f.e. a!=b becomes a=b)
		// in this case we want to get the inverted set of meta records that matches
		// "expr", because when generating a off of this smaller result set less meta
		// record expressions will need to be checked, which will improve the filter
		// speed. if the meta record filter set has been generated from an inverted
		// expression, then its result will need to be inverted again to get the
		// correct result.
		// f.e. if a!=b previously has been inverted to a=b, and for a given MD a=b
		// results in true, then a!=b would result in false
		invertSetOfMetaRecords := expr.ResultIsSmallerWhenInverted()

		// if no meta records match this expression, then we don't need to generate
		// a meta record filter for it
		metaRecordIds := ctx.metaTagIndex.getMetaRecordIdsByExpression(expr, invertSetOfMetaRecords)
		if len(metaRecordIds) == 0 {
			continue
		}

		var metaRecordFilters []tagquery.MetricDefinitionFilter
		for _, id := range metaRecordIds {
			record, ok := ctx.metaTagRecords.records[id]
			if !ok {
				corruptIndex.Inc()
				log.Errorf("TagQueryContext: Tried to lookup a meta tag record id that does not exist, index is corrupted")
				continue
			}

			metaRecordFilters = append(metaRecordFilters, record.GetMetricDefinitionFilter(ctx.index.idHasTag))
		}

		if invertSetOfMetaRecords {
			res.filters[i].testByMetaTags = metaRecordFilterInverted(metaRecordFilters, res.filters[i].defaultDecision)
		} else {
			res.filters[i].testByMetaTags = metaRecordFilterNormal(metaRecordFilters, res.filters[i].defaultDecision)
		}
	}

	return &res
}

func metaRecordFilterInverted(metaRecordFilters []tagquery.MetricDefinitionFilter, defaultDecision tagquery.FilterDecision) tagquery.MetricDefinitionFilter {
	return func(id schema.MKey, name string, tags []string) tagquery.FilterDecision {
		for _, metaRecordFilter := range metaRecordFilters {
			decision := metaRecordFilter(id, name, tags)
			if decision == tagquery.None {
				decision = defaultDecision
			}

			if decision == tagquery.Fail {
				return tagquery.Pass
			}
		}

		return tagquery.Fail
	}
}

func metaRecordFilterNormal(metaRecordFilters []tagquery.MetricDefinitionFilter, defaultDecision tagquery.FilterDecision) tagquery.MetricDefinitionFilter {
	return func(id schema.MKey, name string, tags []string) tagquery.FilterDecision {
		for _, metaRecordFilter := range metaRecordFilters {
			decision := metaRecordFilter(id, name, tags)
			if decision == tagquery.None {
				decision = defaultDecision
			}

			if decision == tagquery.Pass {
				return tagquery.Pass
			}
		}

		return tagquery.Fail
	}
}

// matches takes descriptive properties of a metric definition and runs them through all
// filters required to come to a decision whether this metric definition should be part
// of the result or not.
// it uses the filter functions that have previously been generated when this instance
// of idFilter was instantiated
func (f *idFilter) matches(id schema.MKey, name string, tags []string) bool {
	for i := range f.filters {
		decision := f.filters[i].testByMetricTags(id, name, tags)

		if decision == tagquery.Pass {
			continue
		}

		if decision == tagquery.Fail {
			return false
		}

		if f.filters[i].testByMetaTags != nil {
			decision = f.filters[i].testByMetaTags(id, name, tags)
		}

		if decision == tagquery.None {
			decision = f.filters[i].defaultDecision
		}

		if decision == tagquery.Pass {
			continue
		}

		return false
	}

	return true
}
