package memory

import (
	"math"
	"sort"
	"strings"
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
	metaTagIndex   metaTagIndex                 // the meta tag index
	metaTagRecords metaTagRecords               // meta tag records keyed by their recordID
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
	q.filter = newIdFilter(filterExpressions, q)
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
func (q *TagQueryContext) RunNonBlocking(index TagIndex, byId map[schema.MKey]*idx.Archive, mti metaTagIndex, mtr metaTagRecords, resCh chan schema.MKey) {
	q.run(index, byId, mti, mtr, resCh)

	go func() {
		q.wg.Wait()
		close(resCh)
	}()
}

// RunBlocking is very similar to RunNonBlocking, but there are two notable differences:
// 1) It only returns once the query execution is complete
// 2) It does not close the resCh which has been passed to it on completion
func (q *TagQueryContext) RunBlocking(index TagIndex, byId map[schema.MKey]*idx.Archive, mti metaTagIndex, mtr metaTagRecords, resCh chan schema.MKey) {
	q.run(index, byId, mti, mtr, resCh)

	q.wg.Wait()
}

// run implements the common parts of RunNonBlocking and RunBlocking
func (q *TagQueryContext) run(index TagIndex, byId map[schema.MKey]*idx.Archive, mti metaTagIndex, mtr metaTagRecords, resCh chan schema.MKey) {
	q.index = index
	q.byId = byId
	q.metaTagIndex = mti
	q.metaTagRecords = mtr
	q.prepareExpressions()

	// no initial expression has been chosen, returning empty result
	if q.startWith < 0 || q.startWith >= len(q.query.Expressions) {
		return
	}

	idCh, _ := q.selector.getIds()

	// start the tag query workers. they'll consume the ids on the idCh and
	// evaluate for each of them whether it satisfies all the conditions
	// defined in the query expressions. those that satisfy all conditions
	// will be pushed into the resCh
	q.wg.Add(TagQueryWorkers)
	for i := 0; i < TagQueryWorkers; i++ {
		go q.filterIdsFromChan(idCh, resCh)
	}
}

// getMaxTagCount calculates the maximum number of results (cardinality) a
// tag query could possibly return
// this is useful because when running a tag query we can abort it as soon as
// we know that there can't be more tags discovered and added to the result set
func (q *TagQueryContext) getMaxTagCount() int {
	defer q.wg.Done()

	tagClause := q.query.GetTagClause()
	if tagClause == nil {
		return len(q.index)
	}

	var maxTagCount int
	for tag := range q.index {
		if tagClause.Matches(tag) {
			maxTagCount++
		}
	}

	return maxTagCount
}

// filterTagsFromChan takes a channel of metric IDs and evaluates each of them
// according to the criteria associated with this query
// those that pass all the tests will have their relevant tags extracted, which
// are then pushed into the given tag channel
func (q *TagQueryContext) filterTagsFromChan(idCh chan schema.MKey, tagCh chan string, stopCh chan struct{}, omitTagFilters bool) {
	// used to prevent that this worker thread will push the same result into
	// the chan twice
	resultsCache := make(map[string]struct{})
	tagClause := q.query.GetTagClause()

IDS:
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

		// generate a set of all tags of the current metric that satisfy the
		// tag filter condition
		metricTags := make(map[string]struct{}, 0)
		for _, tag := range def.Tags {
			equal := strings.Index(tag, "=")
			if equal < 0 {
				corruptIndex.Inc()
				log.Errorf("memory-idx: ID %q has tag %q in index without '=' sign", id, tag)
				continue
			}

			key := tag[:equal]
			// this tag has already been pushed into tagCh, so we can stop evaluating
			if _, ok := resultsCache[key]; ok {
				continue
			}

			if tagClause != nil && !tagClause.Matches(key) {
				continue
			}

			metricTags[key] = struct{}{}
		}

		// if we don't filter tags, then we can assume that "name" should always be part of the result set
		if omitTagFilters {
			if _, ok := resultsCache["name"]; !ok {
				metricTags["name"] = struct{}{}
			}
		}

		// if some tags satisfy the current tag filter condition then we run
		// the metric through all tag expression tests in order to decide
		// whether those tags should be part of the final result set
		if len(metricTags) > 0 {
			if q.testByFrom(def) && q.filter.matches(id, schema.SanitizeNameAsTagValue(def.Name), def.Tags) {
				for key := range metricTags {
					select {
					case tagCh <- key:
					case <-stopCh:
						// if execution of query has stopped because the max tag
						// count has been reached then tagCh <- might block
						// because that channel will not be consumed anymore. in
						// that case the stop channel will have been closed so
						// we so we exit here
						break IDS
					}
					resultsCache[key] = struct{}{}
				}
			} else {
				// check if we need to stop
				select {
				case <-stopCh:
					break IDS
				default:
				}
			}
		}
	}

	q.wg.Done()
}

// determines whether the given tag prefix/tag match will match the special
// tag "name". if it does, then we can omit some filtering because we know
// that every metric has a name
func (q *TagQueryContext) tagFilterMatchesName() bool {
	tagClause := q.query.GetTagClause()

	// some tag queries might have no prefix specified yet, in this case
	// we do not need to filter by the name
	// f.e. we know that every metric has a name, and we know that the
	// prefix "" matches the string "name", so we know that every metric
	// will pass the tag prefix test. hence we can omit the entire test.
	if tagClause == nil {
		return true
	}

	return tagClause.Matches("name")
}

// RunGetTags executes the tag query and returns all the tags of the
// resulting metrics
func (q *TagQueryContext) RunGetTags(index TagIndex, byId map[schema.MKey]*idx.Archive, mti metaTagIndex, mtr metaTagRecords) map[string]struct{} {
	q.index = index
	q.byId = byId
	q.metaTagIndex = mti
	q.metaTagRecords = mtr
	q.prepareExpressions()

	maxTagCount := int32(math.MaxInt32)

	// start a thread to calculate the maximum possible number of tags.
	// this might not always complete before the query execution, but in most
	// cases it likely will. when it does end before the execution of the query,
	// the value of maxTagCount will be used to abort the query execution once
	// the max number of possible tags has been reached
	q.wg.Add(1)
	go atomic.StoreInt32(&maxTagCount, int32(q.getMaxTagCount()))

	idCh, stopCh := q.selector.getIds()
	tagCh := make(chan string)

	// we know there can only be 1 tag filter, so if we detect that the given
	// tag condition matches the special tag "name", we can omit the filtering
	// because every metric has a name.
	matchName := q.tagFilterMatchesName()

	// start the tag query workers. they'll consume the ids on the idCh and
	// evaluate for each of them whether it satisfies all the conditions
	// defined in the query expressions. then they will extract the tags of
	// those that satisfy all conditions and push them into tagCh.
	q.wg.Add(TagQueryWorkers)
	for i := 0; i < TagQueryWorkers; i++ {
		go q.filterTagsFromChan(idCh, tagCh, stopCh, matchName)
	}

	go func() {
		q.wg.Wait()
		close(tagCh)
	}()

	result := make(map[string]struct{})

	for tag := range tagCh {
		result[tag] = struct{}{}

		// if we know that there can't be more results than what we have
		// abort the query execution
		if int32(len(result)) >= atomic.LoadInt32(&maxTagCount) {
			break
		}
	}

	// abort query execution and wait for all workers to end
	close(stopCh)

	q.wg.Wait()
	return result
}
