package memory

import (
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/raintank/schema"

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

	query   tagquery.Query
	filters []filter

	index       TagIndex                     // the tag index, hierarchy of tags & values, set by Run()/RunGetTags()
	byId        map[schema.MKey]*idx.Archive // the metric index by ID, set by Run()/RunGetTags()
	mti         metaTagIndex                 // the meta tag index
	metaRecords metaTagRecords               // meta tag records keyed by their recordID
	startWith   int                          // the expression index to start with
	subQuery    bool                         // true if this is a subquery created from the expressions of a meta tag record
}

type filter struct {
	// expr is the expression based on which this filter has been generated
	expr tagquery.Expression

	// test is a filter function which takes a MetricDefinition and returns a
	// tagquery.FilterDecision type indicating whether the MD
	// satisfies this expression or not
	test tagquery.MetricDefinitionFilter

	// testByMetaTags is a filter function which has been generated
	// from the meta records that match this filter's expression
	testByMetaTags tagquery.MetricDefinitionFilter

	// the default decision which should be applied if none of test & testByMetaTags
	// have come to a conclusive decision
	defaultDecision tagquery.FilterDecision
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

func (q *TagQueryContext) prepareExpressions(idx TagIndex) {
	type expressionCost struct {
		operatorCost  uint32
		cardinality   uint32
		expressionIdx int
	}
	costs := make([]expressionCost, len(q.query.Expressions))

	for i, expr := range q.query.Expressions {
		costs[i].expressionIdx = i

		if expr.OperatesOnTag() {
			if expr.MatchesExactly() {
				costs[i].operatorCost = expr.GetOperatorCost()
				costs[i].cardinality = uint32(len(idx[expr.GetKey()]))
			} else {
				costs[i].operatorCost = expr.GetOperatorCost()
				costs[i].cardinality = uint32(len(idx))
			}
		} else {
			if expr.MatchesExactly() {
				costs[i].operatorCost = expr.GetOperatorCost()
				costs[i].cardinality = uint32(len(idx[expr.GetKey()][expr.GetValue()]))
			} else {
				costs[i].operatorCost = expr.GetOperatorCost()
				costs[i].cardinality = uint32(len(idx[expr.GetKey()]))
			}
		}
	}

	sort.Slice(costs, func(i, j int) bool {
		if costs[i].operatorCost == costs[j].operatorCost {
			return costs[i].cardinality < costs[j].cardinality
		}
		return costs[i].operatorCost < costs[j].operatorCost
	})

	// the number of filters is equal to the number of expressions - 1 because one of the
	// expressions will be chosen to be the one that we start with.
	// we don't need the filter function, nor the default decision, of the expression which
	// we start with.
	// all the remaining expressions will be used as filter expressions, for which we need
	// to obtain their filter functions and their default decisions.
	q.filters = make([]filter, len(q.query.Expressions)-1)

	// Every tag query has at least one expression which requires a non-empty value according to:
	// https://graphite.readthedocs.io/en/latest/tags.html#querying
	// This rule is enforced by tagquery.NewQuery, here we trust that the queries which get passed
	// into the index have already been validated
	i := 0
	for _, cost := range costs {
		if q.startWith < 0 && q.query.Expressions[cost.expressionIdx].RequiresNonEmptyValue() {
			q.startWith = cost.expressionIdx
		} else {
			expr := q.query.Expressions[cost.expressionIdx]
			q.filters[i] = filter{
				expr:            expr,
				test:            expr.GetMetricDefinitionFilter(idx.idHasTag),
				defaultDecision: expr.GetDefaultDecision(),
			}
			if tagquery.MetaTagSupport {
				recordIds := q.mti.getMetaRecordIdsByExpression(expr)
				var expressionFilters []tagquery.MetricDefinitionFilter
				for _, id := range recordIds {
					record, ok := q.metaRecords[id]
					if !ok {
						corruptIndex.Inc()
						log.Errorf("TagQueryContext: Tried to lookup a meta tag record id that does not exist, index is corrupted")
						continue
					}

					expressionFilters = append(expressionFilters, record.GetMetricDefinitionFilter(idx.idHasTag))
				}

				if expr.ResultIsSmallerWhenNegated() {
					q.filters[i].testByMetaTags = func(id schema.MKey, name string, tags []string) tagquery.FilterDecision {
						for _, expressionFilter := range expressionFilters {
							decision := expressionFilter(id, name, tags)
							if decision == tagquery.None {
								decision = q.filters[i].expr.GetDefaultDecision()
							}

							if decision == tagquery.Fail {
								return tagquery.Pass
							}
						}

						return tagquery.Fail
					}
				} else {
					q.filters[i].testByMetaTags = func(id schema.MKey, name string, tags []string) tagquery.FilterDecision {
						for _, expressionFilter := range expressionFilters {
							decision := expressionFilter(id, name, tags)
							if decision == tagquery.None {
								decision = q.filters[i].expr.GetDefaultDecision()
							}

							if decision == tagquery.Fail {
								return tagquery.Fail
							}
						}
						return tagquery.Pass
					}
				}
			}
			i++
		}
	}
}

// getInitialIds asynchronously collects all ID's of the initial result set.  It returns:
// a channel through which the IDs of the initial result set will be sent
// a stop channel, which when closed, will cause it to abort the background worker.
func (q *TagQueryContext) getInitialIds() (chan schema.MKey, chan struct{}) {
	idCh := make(chan schema.MKey, 1000)
	stopCh := make(chan struct{})

	if q.query.Expressions[q.startWith].OperatesOnTag() {
		q.getInitialByTag(idCh, stopCh)
	} else {
		q.getInitialByTagValue(idCh, stopCh)
	}

	return idCh, stopCh
}

// getInitialByTagValue generates an initial ID set which is later filtered down
// it only handles those expressions which involve matching a tag value:
// f.e. key=value but not key!=
func (q *TagQueryContext) getInitialByTagValue(idCh chan schema.MKey, stopCh chan struct{}) {
	expr := q.query.Expressions[q.startWith]

	// this wait group is used to wait for all id producing go routines to complete
	// their respective jobs, once they're done we can close the id chan
	closeIdChanWg := sync.WaitGroup{}
	closeIdChanWg.Add(2)
	go func() {
		closeIdChanWg.Wait()
		close(idCh)
	}()

	// query the metric tag index for the initial query expression
	q.wg.Add(1)
	go func() {
		defer closeIdChanWg.Done()
		defer q.wg.Done()

		q.getInitialByTagValueFromMetricTagIndex(idCh, stopCh, expr)
	}()

	// if this is a sub query we want to ignore the meta tag index,
	// otherwise we'd risk to create a loop of sub queries creating
	// each other
	if tagquery.MetaTagSupport && !q.subQuery {
		// query the meta tag index for the initial query expression
		q.wg.Add(1)
		go func() {
			defer closeIdChanWg.Done()
			defer q.wg.Done()

			q.getInitialByTagValueFromMetaTagIndex(idCh, stopCh, expr, &closeIdChanWg)
		}()
	} else {
		closeIdChanWg.Done()
	}
}

func (q *TagQueryContext) getInitialByTagValueFromMetricTagIndex(idCh chan schema.MKey, stopCh chan struct{}, expr tagquery.Expression) {
	if expr.MatchesExactly() {
		for id := range q.index[expr.GetKey()][expr.GetValue()] {
			select {
			case <-stopCh:
				break
			case idCh <- id:
			}
		}
	} else {
	OUTER:
		for value, ids := range q.index[expr.GetKey()] {
			if !expr.Matches(value) {
				continue
			}

			for id := range ids {
				select {
				case <-stopCh:
					break OUTER
				case idCh <- id:
				}
			}
		}
	}
}

func (q *TagQueryContext) getInitialByTagValueFromMetaTagIndex(idCh chan schema.MKey, stopCh chan struct{}, expr tagquery.Expression, closeIdChanWg *sync.WaitGroup) {
OUTER:
	for value, records := range q.mti[expr.GetKey()] {
		select {
		// abort if query has been stopped
		case <-stopCh:
			break OUTER
		default:
		}

		if !expr.Matches(value) {
			continue
		}

		for _, metaRecordId := range records {
			record, ok := q.metaRecords[metaRecordId]
			if !ok {
				corruptIndex.Inc()
				continue
			}

			closeIdChanWg.Add(1)
			go func() {
				defer closeIdChanWg.Done()

				query, err := q.subQueryFromExpressions(record.Expressions)
				if err != nil {
					return
				}

				resCh := query.Run(q.index, q.byId, q.mti, q.metaRecords)

				for id := range resCh {
					select {
					// abort if query has been stopped
					case <-stopCh:
						break
					case idCh <- id:
					}
				}
			}()
		}
	}
}

func (q *TagQueryContext) subQueryFromExpressions(expressions tagquery.Expressions) (TagQueryContext, error) {
	var queryCtx TagQueryContext

	query, err := tagquery.NewQuery(expressions, q.query.From)
	if err != nil {
		// this means we've stored a meta record containing invalid queries
		corruptIndex.Inc()
		return queryCtx, err
	}

	queryCtx = NewTagQueryContext(query)
	queryCtx.subQuery = true

	return queryCtx, nil
}

// getInitialByTag generates an initial ID set which is later filtered down
// it only handles those expressions which do not involve matching a tag value:
// f.e. key!= but not key=value
func (q *TagQueryContext) getInitialByTag(idCh chan schema.MKey, stopCh chan struct{}) {
	expr := q.query.Expressions[q.startWith]

	q.wg.Add(1)
	go func() {
		defer close(idCh)
		defer q.wg.Done()

		if expr.MatchesExactly() {
			for _, ids := range q.index[expr.GetKey()] {
				for id := range ids {
					select {
					case <-stopCh:
						break
					case idCh <- id:
					}
				}
			}
		} else {
		OUTER:
			for tag := range q.index {
				if !expr.Matches(tag) {
					continue
				}

				for _, ids := range q.index[tag] {
					for id := range ids {
						select {
						case <-stopCh:
							break OUTER
						case idCh <- id:
						}
					}
				}
			}
		}
	}()
}

// testByAllExpressions takes and id and a MetricDefinition and runs it through
// all required tests in order to decide whether this metric should be part
// of the final result set or not
// in map/reduce terms this is the reduce function
func (q *TagQueryContext) testByAllExpressions(id schema.MKey, def *idx.Archive, omitTagFilters bool) bool {
	if !q.testByFrom(def) {
		return false
	}

	for i := range q.filters {
		decision := q.filters[i].test(id, schema.SanitizeNameAsTagValue(def.Name), def.Tags)

		if decision == tagquery.Pass {
			continue
		}

		if decision == tagquery.Fail {
			return false
		}

		decision = q.filters[i].testByMetaTags(id, def.Name, def.Tags)

		if decision == tagquery.None {
			decision = q.filters[i].defaultDecision
		}

		if decision == tagquery.Pass {
			continue
		}

		return false
	}

	return true
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

		// we always omit tag filters because Run() does not support filtering by tags
		if q.testByAllExpressions(id, def, false) {
			resCh <- id
		}
	}

	q.wg.Done()
}

// Run executes the tag query on the given index and returns a list of ids
func (q *TagQueryContext) Run(index TagIndex, byId map[schema.MKey]*idx.Archive, mti metaTagIndex, metaRecords metaTagRecords) IdSet {
	q.index = index
	q.byId = byId
	q.mti = mti
	q.metaRecords = metaRecords
	q.prepareExpressions(index)

	idCh, _ := q.getInitialIds()
	resCh := make(chan schema.MKey)

	// start the tag query workers. they'll consume the ids on the idCh and
	// evaluate for each of them whether it satisfies all the conditions
	// defined in the query expressions. those that satisfy all conditions
	// will be pushed into the resCh
	q.wg.Add(TagQueryWorkers)
	for i := 0; i < TagQueryWorkers; i++ {
		go q.filterIdsFromChan(idCh, resCh)
	}

	go func() {
		q.wg.Wait()
		close(resCh)
	}()

	result := make(IdSet)

	for id := range resCh {
		result[id] = struct{}{}
	}

	return result
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
			if q.testByAllExpressions(id, def, omitTagFilters) {
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
func (q *TagQueryContext) RunGetTags(index TagIndex, byId map[schema.MKey]*idx.Archive, mti metaTagIndex, metaRecords metaTagRecords) map[string]struct{} {
	q.index = index
	q.byId = byId
	q.mti = mti
	q.metaRecords = metaRecords
	q.prepareExpressions(index)

	maxTagCount := int32(math.MaxInt32)

	// start a thread to calculate the maximum possible number of tags.
	// this might not always complete before the query execution, but in most
	// cases it likely will. when it does end before the execution of the query,
	// the value of maxTagCount will be used to abort the query execution once
	// the max number of possible tags has been reached
	q.wg.Add(1)
	go atomic.StoreInt32(&maxTagCount, int32(q.getMaxTagCount()))

	idCh, stopCh := q.getInitialIds()
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
