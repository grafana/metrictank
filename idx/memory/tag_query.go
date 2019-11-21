package memory

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/grafana/metrictank/schema"

	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx"
	log "github.com/sirupsen/logrus"
)

// TagQueryContext runs a set of pattern or string matches on tag keys and values against
// the index. It is executed via:
// Run() which returns a set of matching MetricIDs
// RunGetTags() which returns a list of tags of the matching metrics
type TagQueryContext struct {
	wg sync.WaitGroup

	query    tagquery.Query
	selector *idSelector
	filter   *idFilter

	index          TagIndex                     // the tag index, hierarchy of tags & values, set by Run()/RunGetTags()
	byId           map[schema.MKey]*idx.Archive // the metric index by ID, set by Run()/RunGetTags()
	metaTagIndex   metaTagHierarchy             // the meta tag index
	metaTagRecords *metaTagRecords              // meta tag records keyed by their recordID
	startWith      int                          // the expression index to start with
	subQuery       bool                         // true if this is a subquery created from the expressions of a meta tag record
}

// NewTagQueryContext takes a tag query and wraps it into all the
// context structs necessary to execute the query on the indexes
func NewTagQueryContext(query tagquery.Query) TagQueryContext {
	ctx := TagQueryContext{
		query:     query,
		startWith: -1,
	}

	return ctx
}

type expressionCost struct {
	operatorCost  uint32
	cardinality   uint32
	expressionIdx int
}

// newerThanFrom takes a metric key, it returns true if the lastUpdate
// property of the metric associated with that key is at least equal
// to this queries' from timestamp.
// if the key doesn't exist it returns false
func (q *TagQueryContext) newerThanFrom(id schema.MKey) bool {
	md, ok := q.byId[id]
	if !ok {
		return false
	}
	return atomic.LoadInt64(&md.LastUpdate) >= q.query.From
}

func (q *TagQueryContext) evaluateExpressionCosts() []expressionCost {
	costs := make([]expressionCost, len(q.query.Expressions))

	for i, expr := range q.query.Expressions {
		costs[i].expressionIdx = i

		if expr.OperatesOnTag() {
			if expr.MatchesExactly() {
				costs[i].operatorCost = expr.GetOperatorCost()
				costs[i].cardinality = uint32(len(q.index[expr.GetKey()]))
			} else {
				costs[i].operatorCost = expr.GetOperatorCost()
				costs[i].cardinality = uint32(len(q.index))
			}
		} else {
			if expr.MatchesExactly() {
				costs[i].operatorCost = expr.GetOperatorCost()
				costs[i].cardinality = uint32(len(q.index[expr.GetKey()][expr.GetValue()]))
			} else {
				costs[i].operatorCost = expr.GetOperatorCost()
				costs[i].cardinality = uint32(len(q.index[expr.GetKey()]))
			}
		}
	}

	sort.Slice(costs, func(i, j int) bool {
		if costs[i].operatorCost == costs[j].operatorCost {
			return costs[i].cardinality < costs[j].cardinality
		}
		return costs[i].operatorCost < costs[j].operatorCost
	})

	return costs
}

func (q *TagQueryContext) prepareExpressions() {
	costs := q.evaluateExpressionCosts()

	// the number of filters is equal to the number of expressions - 1 because one of the
	// expressions will be chosen to be the one that we start with.
	// we don't need the filter function, nor the default decision, of the expression which
	// we start with.
	// all the remaining expressions will be used as filter expressions, for which we need
	// to obtain their filter functions and their default decisions.
	filterExpressions := make([]tagquery.Expression, 0, len(q.query.Expressions)-1)

	// Every tag query has at least one expression which requires a non-empty value according to:
	// https://graphite.readthedocs.io/en/latest/tags.html#querying
	// This rule is enforced by tagquery.NewQuery, here we trust that the queries which get passed
	// into the index have already been validated
	for _, cost := range costs {
		if q.startWith < 0 && q.query.Expressions[cost.expressionIdx].RequiresNonEmptyValue() {
			q.startWith = cost.expressionIdx
		} else {
			filterExpressions = append(filterExpressions, q.query.Expressions[cost.expressionIdx])
		}
	}

	q.selector = newIdSelector(q.query.Expressions[q.startWith], q)
	if len(filterExpressions) > 0 {
		q.filter = newIdFilter(filterExpressions, q)
	}
}

// testByFrom filters a given metric by its LastUpdate time
func (q *TagQueryContext) testByFrom(def *idx.Archive) bool {
	return q.query.From <= atomic.LoadInt64(&def.LastUpdate)
}

// filterIdsFromChan takes a channel of metric ids and runs them through the
// required tests to decide whether a metric should be part of the final
// result set or not
// it returns the final result set via the given resCh parameter
func (q *TagQueryContext) filterIdsFromChan(idCh, resCh chan schema.MKey) {
	for id := range idCh {
		var def *idx.Archive
		var ok bool

		if def, ok = q.byId[id]; !ok {
			// should never happen because every ID in the tag index
			// must be present in the byId lookup table
			corruptIndex.Inc()
			log.Errorf("memory-idx: ID %q is in tag index but not in the byId lookup table", id)
			continue
		}

		if q.testByFrom(def) && q.filter.matches(id, schema.SanitizeNameAsTagValue(def.Name), def.Tags) {
			resCh <- id
		}
	}

	q.wg.Done()
}

// RunNonBlocking executes the tag query on the given index and returns a list of ids
// It takes the following arguments:
// index:	    the tag index to operate on
// byId:        a map keyed by schema.MKey referring to *idx.Archive
// mti:         the meta tag index
// mtr:         the meta tag records
// resCh:       a chan of schema.MKey into which the result set will be pushed
//              this channel gets closed when the query execution is complete
func (q *TagQueryContext) RunNonBlocking(index TagIndex, byId map[schema.MKey]*idx.Archive, mti metaTagHierarchy, mtr *metaTagRecords, resCh chan schema.MKey) {
	q.run(index, byId, mti, mtr, resCh)

	go func() {
		q.wg.Wait()
		close(resCh)
	}()
}

// RunBlocking is very similar to RunNonBlocking, but there are two notable differences:
// 1) It only returns once the query execution is complete
// 2) It does not close the resCh which has been passed to it on completion
func (q *TagQueryContext) RunBlocking(index TagIndex, byId map[schema.MKey]*idx.Archive, mti metaTagHierarchy, mtr *metaTagRecords, resCh chan schema.MKey) {
	q.run(index, byId, mti, mtr, resCh)

	q.wg.Wait()
}

// run implements the common parts of RunNonBlocking and RunBlocking
func (q *TagQueryContext) run(index TagIndex, byId map[schema.MKey]*idx.Archive, mti metaTagHierarchy, mtr *metaTagRecords, resCh chan schema.MKey) {
	q.index = index
	q.byId = byId
	q.metaTagIndex = mti
	q.metaTagRecords = mtr
	q.prepareExpressions()

	// no initial expression has been chosen, returning empty result
	if q.startWith < 0 || q.startWith >= len(q.query.Expressions) {
		return
	}

	// if this query needs to filter down the initial result set, which is
	// only the case if the number of expressions is >1, then we start filter
	// workers to apply the filter functions
	if q.filter != nil {
		idCh := make(chan schema.MKey)
		q.wg.Add(1)
		go func() {
			defer q.wg.Done()
			q.selector.getIds(idCh, nil)
			close(idCh)
		}()

		// start the tag query workers. they'll consume the ids on the idCh and
		// evaluate for each of them whether it satisfies all the conditions
		// defined in the query expressions. those that satisfy all conditions
		// will be pushed into the resCh
		q.wg.Add(TagQueryWorkers)
		for i := 0; i < TagQueryWorkers; i++ {
			go q.filterIdsFromChan(idCh, resCh)
		}
	} else {
		q.wg.Add(1)
		go func() {
			defer q.wg.Done()
			q.selector.getIds(resCh, nil)
		}()
	}
}
