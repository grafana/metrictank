package memory

import (
	"sort"
	"strings"

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
		onlyEqualOperators := !invertSetOfMetaRecords
		singleEqualExprPerRecord := true
		records := make([]tagquery.MetaTagRecord, len(metaRecordIds))
		for i, id := range metaRecordIds {
			record, ok := ctx.metaTagRecords.records[id]
			if !ok {
				corruptIndex.Inc()
				log.Errorf("TagQueryContext: Tried to lookup a meta tag record id that does not exist, index is corrupted")
				continue
			}

			if onlyEqualOperators {
				for exprIdx := range record.Expressions {
					if record.Expressions[exprIdx].GetOperator() != tagquery.EQUAL {
						onlyEqualOperators = false
						break
					}
				}
				records[i] = record
				if len(record.Expressions) > 1 {
					singleEqualExprPerRecord = false
				}
			}

			metaRecordFilters = append(metaRecordFilters, record.GetMetricDefinitionFilter(ctx.index.idHasTag))
		}

		if onlyEqualOperators {
			if singleEqualExprPerRecord {
				res.filters[i].testByMetaTags = metaRecordFilterBySetOfValidValues(records)
			} else {
				res.filters[i].testByMetaTags = metaRecordFilterBySetOfValidValueSets(records)
			}
		} else {
			if invertSetOfMetaRecords {
				res.filters[i].testByMetaTags = metaRecordFilterInverted(metaRecordFilters, res.filters[i].defaultDecision)
			} else {
				res.filters[i].testByMetaTags = metaRecordFilterNormal(metaRecordFilters, res.filters[i].defaultDecision)
			}
		}
	}

	return &res
}

func metaRecordFilterBySetOfValidValues(records []tagquery.MetaTagRecord) tagquery.MetricDefinitionFilter {
	validValues := make(map[string]struct{})
	validNames := make(map[string]struct{})
	var builder strings.Builder
	for i := range records {
		if records[i].Expressions[0].GetKey() == "name" {
			validNames[records[i].Expressions[0].GetValue()] = struct{}{}
		} else {
			records[i].Expressions[0].StringIntoWriter(&builder)
			validValues[builder.String()] = struct{}{}
			builder.Reset()
		}
	}

	return func(_ schema.MKey, name string, tags []string) tagquery.FilterDecision {
		for i := range tags {
			if _, ok := validValues[tags[i]]; ok {
				return tagquery.Pass
			}
		}
		if _, ok := validNames[name]; ok {
			return tagquery.Pass
		}
		return tagquery.None
	}
}

func metaRecordFilterBySetOfValidValueSets(records []tagquery.MetaTagRecord) tagquery.MetricDefinitionFilter {
	validValueSets := make([]struct {
		name string
		tags []string
	}, len(records))

	var builder strings.Builder
	for i := range records {
		validValueSets[i].tags = make([]string, 0, len(records[i].Expressions))
		for j := range records[i].Expressions {
			if records[i].Expressions[j].GetKey() == "name" {
				validValueSets[i].name = records[i].Expressions[j].GetValue()
			} else {
				records[i].Expressions[j].StringIntoWriter(&builder)
				validValueSets[i].tags = append(validValueSets[i].tags, builder.String())
				builder.Reset()
			}
		}
		sort.Strings(validValueSets[i].tags)
	}

	return func(_ schema.MKey, name string, tags []string) tagquery.FilterDecision {
		for _, validValueSet := range validValueSets {
			if len(validValueSet.name) > 0 {
				if name != validValueSet.name {
					continue
				}
			}

			if sliceContainsElements(validValueSet.tags, tags) {
				return tagquery.Pass
			}
		}

		return tagquery.None
	}
}

// sliceContainsElements returns true if the elements in the slice "find"
// are all present int the slice "in". It requires both slices to be sorted
func sliceContainsElements(find, in []string) bool {
	var findIdx, inIdx int
	for {
		if findIdx == len(find) {
			return true
		}

		if inIdx == len(in) {
			return false
		}

		if find[findIdx] == in[inIdx] {
			findIdx++
			inIdx++
			continue
		}

		if find[findIdx] < in[inIdx] {
			return false
		}

		inIdx++
	}
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
