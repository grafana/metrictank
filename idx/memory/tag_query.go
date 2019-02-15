package memory

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/raintank/schema"

	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/idx/memory/tagQueryExpression"
	log "github.com/sirupsen/logrus"
)

var (
	errInvalidQuery = errors.New("invalid query")
)

// the supported operators are documented together with the graphite
// reference implementation:
// http://graphite.readthedocs.io/en/latest/tags.html
//
// some of the following operators are non-standard and are only used
// internally to implement certain functionalities requiring them

// a key / value combo used to represent a tag expression like "key=value"
type kv struct {
	key   string
	value string
}

func (k *kv) stringIntoBuilder(builder *strings.Builder) {
	builder.WriteString(k.key)
	builder.WriteString("=")
	builder.WriteString(k.value)
}

type filter struct {
	// expr is the expression based on which this filter has been generated
	expr tagQueryExpression.Expression

	// test is a filter function which takes a MetricDefinition and returns a
	// tagQueryExpression.FilterDecision type indicating whether the MD
	// satisfies this expression or not
	test tagQueryExpression.MetricDefinitionFilter

	// testByMetaTags is a filter function which has been generated from the
	// meta records that match this filter's expression. It accepts a set of
	// tags and returns true if the given set of tags should pass this filter
	// due to matching meta records, otherwise false
	testByMetaTags func(map[string]string) bool

	// indicates whether the meta tag index should be taken into account for
	// this filter
	meta bool
}

// TagQuery runs a set of pattern or string matches on tag keys and values against
// the index. It is executed via:
// Run() which returns a set of matching MetricIDs
// RunGetTags() which returns a list of tags of the matching metrics
type TagQuery struct {
	// clause that operates on LastUpdate field
	from    int64
	filters []filter

	metricExpressions        []tagQueryExpression.Expression
	mixedExpressions         []tagQueryExpression.Expression
	tagQuery                 tagQueryExpression.Expression
	initialExpression        tagQueryExpression.Expression
	initialExpressionUseMeta bool

	index       TagIndex                     // the tag index, hierarchy of tags & values, set by Run()/RunGetTags()
	byId        map[schema.MKey]*idx.Archive // the metric index by ID, set by Run()/RunGetTags()
	metaIndex   metaTagIndex
	metaRecords metaTagRecords

	// indicates whether this query is a subQuery or not
	// f.e. if it has been created to evaluate the query expressions attached to
	//  a meta record which has been matched by the original query, then it's
	// considered a sub-query.
	// sub queries should never take the meta tag index into account
	// to prevent loops.
	subQuery bool
}

func tagMapFromStrings(tags []string) (map[string]string, error) {
	res := make(map[string]string, len(tags))
	var err error
	for _, tag := range tags {
		equal := strings.Index(tag, "=")
		if equal < 0 {
			err = fmt.Errorf("invalid tag string")
			continue
		}
		res[tag[:equal]] = tag[equal:]
	}
	return res, err
}

func tagQueryFromExpressions(expressions []tagQueryExpression.Expression, from int64, subQuery bool) (*TagQuery, error) {
	q := TagQuery{from: from, subQuery: subQuery}

	// every set of expressions must have at least one positive operator (=, =~, ^=, <tag>!=<empty>, __tag^=, __tag=~)
	foundPositiveOperator := false

	// there can never be more than one tag query
	foundTagQuery := false
	for _, e := range expressions {

		if !foundPositiveOperator && e.IsPositiveOperator() {
			foundPositiveOperator = true
		}

		if e.IsTagOperator() {
			if foundTagQuery {
				return nil, errInvalidQuery
			}
			foundTagQuery = true
		}

		q.mixedExpressions = append(q.mixedExpressions, e)
	}

	if !foundPositiveOperator {
		return nil, errInvalidQuery
	}

	return &q, nil
}

// NewTagQuery initializes a new tag query from the given expressions and the
// from timestamp. It assigns all expressions to the expression group for
// metric tags, later when sortByCost is called it will move those out which
// are keyed by a tag that doesn't exist in the metric index.
func NewTagQuery(expressions []string, from int64) (*TagQuery, error) {
	if len(expressions) == 0 {
		return nil, errInvalidQuery
	}

	parsed := make([]tagQueryExpression.Expression, 0, len(expressions))
	sort.Strings(expressions)
	for i, expr := range expressions {
		// skip duplicate expression
		if i > 0 && expr == expressions[i-1] {
			continue
		}

		e, err := tagQueryExpression.ParseExpression(expr)
		if err != nil {
			return nil, err
		}

		parsed = append(parsed, e)
	}

	query, err := tagQueryFromExpressions(parsed, from, false)
	if err != nil {
		return nil, err
	}

	return query, nil
}

// Run executes the tag query on the given index and returns a list of ids
func (q *TagQuery) Run() IdSet {
	res := q.run()

	result := make(IdSet)
	for id := range res {
		result[id] = struct{}{}
	}

	return result
}

func (q *TagQuery) run() chan schema.MKey {
	q.sortByCost()

	prepareFiltersWg := sync.WaitGroup{}
	prepareFiltersWg.Add(1)
	go func() {
		defer prepareFiltersWg.Done()
		q.prepareFilters()
	}()

	initialIds := make(chan schema.MKey, 1000)
	workersWg := sync.WaitGroup{}
	q.getInitialIds(&workersWg, initialIds)

	workersWg.Add(TagQueryWorkers)
	results := make(chan schema.MKey, 10000)
	prepareFiltersWg.Wait()

	// start the tag query workers. they'll consume the ids on the channel
	// initialIds and evaluate for each of them whether it satisfies all the
	// conditions defined in the query expressions. those that satisfy all
	// conditions will be pushed into the results channel
	for i := 0; i < TagQueryWorkers; i++ {
		go q.filterIdsFromChan(&workersWg, initialIds, results)
	}

	go func() {
		workersWg.Wait()
		close(results)
	}()

	return results
}

// initForIndex takes all the index-datastructures and assigns them to this tag query
// if this tag query has been instantiated from a query given by the user, it will
// simply get these structs assigned from idx.UnpartitionedMemoryIndex, if it has been
// instantiated as a sub query to evaluate a meta tag then it gets the data structures
// copied from the parent query
func (q *TagQuery) initForIndex(defById map[schema.MKey]*idx.Archive, idx TagIndex, mti metaTagIndex, mtr metaTagRecords) {
	q.index = idx
	q.byId = defById
	q.metaIndex = mti
	q.metaRecords = mtr
}

// subQueryFromExpressions is used to evaluate a meta tag. when a meta tag needs to
// be evaluated we take its associated query expressions and instantiate a sub-query
// from them
func (q *TagQuery) subQueryFromExpressions(expressions []tagQueryExpression.Expression) (*TagQuery, error) {
	query, err := tagQueryFromExpressions(expressions, q.from, true)
	if err != nil {
		// this means we've stored a meta record containing invalid queries
		corruptIndex.Inc()
		return nil, err
	}

	query.initForIndex(q.byId, q.index, q.metaIndex, q.metaRecords)

	return query, nil
}

// getInitialIds asynchronously collects all IDs of the initial result set.
// It returns a stop channel, which when closed, will cause it to abort the background worker.
func (q *TagQuery) getInitialIds(wg *sync.WaitGroup, idCh chan schema.MKey) chan struct{} {
	stopCh := make(chan struct{})
	wg.Add(1)

	if q.initialExpression.MatchesTag() {
		go q.getInitialByTag(wg, idCh, stopCh)
	} else {
		go q.getInitialByTagValue(wg, idCh, stopCh)
	}

	return stopCh
}

// getInitialByTagValue generates an initial ID set which is later filtered down
// it only handles those expressions which involve matching a tag value:
// f.e. key=value but not key!=
func (q *TagQuery) getInitialByTagValue(wg *sync.WaitGroup, idCh chan schema.MKey, stopCh chan struct{}) {
	key := q.initialExpression.GetKey()
	match := q.initialExpression.GetMatcher()
	initialIdsWg := sync.WaitGroup{}
	initialIdsWg.Add(1)

	go func() {
		defer initialIdsWg.Done()
	IDS:
		for v, ids := range q.index[key] {
			if !match(v) {
				continue
			}

			for id := range ids {
				select {
				case <-stopCh:
					break IDS
				case idCh <- id:
				}
			}
		}
	}()

	// sortByCost() will usually try to not choose an expression that involves
	// meta tags as the initial expression, but if necessary we need to
	// evaluate those too
	// if this is a sub query we want to ignore the meta index to prevent loops
	if !q.subQuery && q.initialExpressionUseMeta {
		for v, records := range q.metaIndex[key] {
			if !match(v) {
				continue
			}

			for _, metaRecordId := range records {
				record, ok := q.metaRecords[metaRecordId]
				if !ok {
					corruptIndex.Inc()
					continue
				}

				initialIdsWg.Add(1)
				go func() {
					defer initialIdsWg.Done()

					query, err := q.subQueryFromExpressions(record.queries)
					if err != nil {
						return
					}

					resCh := query.run()
					for id := range resCh {
						idCh <- id
					}
				}()
			}
		}
	}

	go func() {
		defer close(idCh)
		defer wg.Done()
		initialIdsWg.Wait()
	}()
}

// getInitialByTag generates an initial ID set which is later filtered down
// it only handles those expressions which do not involve matching a tag value:
// f.e. key!= but not key=value
func (q *TagQuery) getInitialByTag(wg *sync.WaitGroup, idCh chan schema.MKey, stopCh chan struct{}) {
	match := q.initialExpression.GetMatcher()
	initialIdsWg := sync.WaitGroup{}
	initialIdsWg.Add(1)

	go func() {
		defer initialIdsWg.Done()
	TAGS:
		for tag, values := range q.index {
			if !match(tag) {
				continue
			}

			for _, ids := range values {
				for id := range ids {
					select {
					case <-stopCh:
						break TAGS
					case idCh <- id:
					}
				}
			}
		}
	}()

	// sortByCost() will usually try to not choose an expression that involves
	// meta tags as the initial expression, but if necessary we need to
	// evaluate those too
	// if this is a sub query we want to ignore the meta index to prevent loops
	if !q.subQuery && q.initialExpressionUseMeta {
		for tag, values := range q.metaIndex {
			if !match(tag) {
				continue
			}

			for _, records := range values {
				for _, metaRecordId := range records {
					record, ok := q.metaRecords[metaRecordId]
					if !ok {
						corruptIndex.Inc()
						continue
					}

					initialIdsWg.Add(1)
					go func() {
						defer initialIdsWg.Done()

						query, err := q.subQueryFromExpressions(record.queries)
						if err != nil {
							return
						}

						resCh := query.run()
						for id := range resCh {
							idCh <- id
						}
					}()
				}
			}
		}
	}

	go func() {
		defer close(idCh)
		defer wg.Done()
		initialIdsWg.Wait()
	}()
}

// testByAllExpressions takes and id and a MetricDefinition and runs it through
// all required tests in order to decide whether this metric should be part
// of the final result set or not
// in map/reduce terms this is the reduce function
func (q *TagQuery) testByAllExpressions(id schema.MKey, def *idx.Archive, omitTagFilters bool) bool {
	if !q.testByFrom(def) {
		return false
	}

	var res tagQueryExpression.FilterDecision
	for _, filter := range q.filters {
		res = filter.test(def)
		if res == tagQueryExpression.Pass {
			continue
		}
		if res == tagQueryExpression.Fail {
			return false
		}

		// if the meta tag index should not be taken into account for this
		// filter we decide based on the expression's default decision
		if !filter.meta {
			if filter.expr.GetDefaultDecision() == tagQueryExpression.Pass {
				continue
			}
			return false
		}

		// if no decision has been made based on the metric tag index, then we
		// evaluate via the meta tag index
		tags, err := tagMapFromStrings(def.Tags)
		if err != nil {
			corruptIndex.Inc()
			return false
		}
		tags["name"] = def.Name

		if !filter.testByMetaTags(tags) {
			return false
		}
	}

	return true
}

// testByFrom filters a given metric by its LastUpdate time
func (q *TagQuery) testByFrom(def *idx.Archive) bool {
	return q.from <= atomic.LoadInt64(&def.LastUpdate)
}

// filterIdsFromChan takes a channel of metric ids and runs them through the
// required tests to decide whether a metric should be part of the final
// result set or not
// it returns the final result set via the given resCh parameter
func (q *TagQuery) filterIdsFromChan(wg *sync.WaitGroup, idCh, resCh chan schema.MKey) {
	defer wg.Done()

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

		if q.testByAllExpressions(id, def, false) {
			resCh <- id
		}
	}
}

func (q *TagQuery) prepareFilters() {
	appendTagQuery := false
	if q.tagQuery != nil && q.tagQuery != q.initialExpression {
		appendTagQuery = true
		q.filters = make([]filter, len(q.metricExpressions)+len(q.mixedExpressions)+1)
	} else {
		q.filters = make([]filter, len(q.metricExpressions)+len(q.mixedExpressions))
	}

	i := 0
	for _, expr := range q.metricExpressions {
		q.filters[i] = filter{
			expr: expr,
			test: expr.GetMetricDefinitionFilter(),
			meta: false,
		}
		i++
	}
	for _, expr := range q.mixedExpressions {
		q.filters[i] = filter{
			expr:           expr,
			test:           expr.GetMetricDefinitionFilter(),
			testByMetaTags: q.getMetaTagFilter(expr),
			meta:           true,
		}
		i++
	}
	if appendTagQuery {
		q.filters[i] = filter{
			expr:           q.tagQuery,
			test:           q.tagQuery.GetMetricDefinitionFilter(),
			testByMetaTags: q.getMetaTagFilter(q.tagQuery),
			meta:           true,
		}
	}
}

// getMetaTagFilter returns a filter function to which a set of tags/values can
// be passed, the filter function is based on the given expression.
// The filter function returns true if the tags/values pass the filter according
// to the meta tag index or false if they don't.
func (q *TagQuery) getMetaTagFilter(expression tagQueryExpression.Expression) func(map[string]string) bool {
	metaRecordIds := q.metaIndex.getMetaRecordIdsByExpression(expression)
	metaTagRecords := q.metaRecords.getRecords(metaRecordIds)

	// in case of positive expression operators, we want to return true if
	// any meta tag record matches the given tag set
	// otherwise, in case of negative ones, we want to return false on match
	resultOnMatch := expression.IsPositiveOperator()

	return func(tags map[string]string) bool {
		for _, record := range metaTagRecords {
			if record.matchesTags(tags) {
				return resultOnMatch
			}
		}
		return !resultOnMatch
	}
}

func (q *TagQuery) sortByCostWithMeta() {
	for i, e := range q.mixedExpressions {
		if e.IsTagOperator() {
			// there can only be one expression using a tag operator per query
			q.tagQuery = e

			q.mixedExpressions = append(q.mixedExpressions[:i], q.mixedExpressions[i+1:]...)
			continue
		}

		// separate all the ones that don't involve the meta index into their
		// own slice, because they should be executed first
		if _, ok := q.metaIndex[e.GetKey()]; !ok {
			q.metricExpressions = append(q.metricExpressions, e)
			q.mixedExpressions = append(q.mixedExpressions[:i], q.mixedExpressions[i+1:]...)
		}
	}

	sort.Slice(q.metricExpressions, func(i, j int) bool {
		costI := len(q.index[q.metricExpressions[i].GetKey()])
		if q.metricExpressions[i].HasRe() {
			costI *= 10
		}

		costJ := len(q.index[q.metricExpressions[j].GetKey()])
		if q.metricExpressions[j].HasRe() {
			costJ *= 10
		}

		return costI < costJ
	})

	sort.Slice(q.mixedExpressions, func(i, j int) bool {
		keyI := q.mixedExpressions[i].GetKey()
		costI := len(q.index[keyI])
		costI += len(q.metaIndex[keyI])
		if q.metricExpressions[i].HasRe() {
			costI *= 10
		}

		keyJ := q.mixedExpressions[j].GetKey()
		costJ := len(q.index[keyJ])
		costJ += len(q.metaIndex[keyJ])
		if q.mixedExpressions[j].HasRe() {
			costJ *= 10
		}

		return costI < costJ
	})
}

func (q *TagQuery) sortByCostWithoutMeta() {
	q.metricExpressions = append(q.metricExpressions, q.mixedExpressions...)
	q.mixedExpressions = nil

	// extract tag query if there is one
	for i, e := range q.metricExpressions {
		if e.IsTagOperator() {
			// there can only be one expression using a tag operator per query
			q.tagQuery = e
			q.metricExpressions = append(q.metricExpressions[:i], q.metricExpressions[i+1:]...)
			continue
		}
	}

	sort.Slice(q.metricExpressions, func(i, j int) bool {
		costI := len(q.index[q.metricExpressions[i].GetKey()])
		if q.metricExpressions[i].HasRe() {
			costI *= 10
		}

		costJ := len(q.index[q.metricExpressions[j].GetKey()])
		if q.metricExpressions[j].HasRe() {
			costJ *= 10
		}

		return costI < costJ
	})
}

func (q *TagQuery) sortByCost() {
	if q.subQuery {
		// if this is a sub query we never want to take the meta tags into account to prevent loops
		q.sortByCostWithoutMeta()
	} else {
		q.sortByCostWithMeta()
	}

	for i, expr := range q.metricExpressions {
		if expr.IsPositiveOperator() {
			q.initialExpression = q.metricExpressions[i]
			q.metricExpressions = append(q.metricExpressions[:i], q.metricExpressions[i+1:]...)
			return
		}
	}
	for i, expr := range q.mixedExpressions {
		if expr.IsPositiveOperator() {
			q.initialExpression = q.mixedExpressions[i]
			q.mixedExpressions = append(q.mixedExpressions[:i], q.mixedExpressions[i+1:]...)
			q.initialExpressionUseMeta = true
			return
		}
	}
	q.initialExpression = q.tagQuery
}

// getMaxTagCount calculates the maximum number of results (cardinality) a
// tag query could possibly return
// this is useful because when running a tag query we can abort it as soon as
// we know that there can't be more tags discovered and added to the result set
func (q *TagQuery) getMaxTagCount(wg *sync.WaitGroup) int {
	defer wg.Done()
	var maxTagCount int
	match := q.tagQuery.GetMatcher()

	for tag := range q.index {
		if match(tag) {
			maxTagCount++
		}
	}

	return maxTagCount
}

// filterTagsFromChan takes a channel of metric IDs and evaluates each of them
// according to the criteria associated with this query
// those that pass all the tests will have their relevant tags extracted, which
// are then pushed into the given tag channel
func (q *TagQuery) filterTagsFromChan(wg *sync.WaitGroup, idCh chan schema.MKey, tagCh chan string, stopCh chan struct{}, omitTagFilters bool) {
	defer wg.Done()

	// used to prevent that this worker thread will push the same result into
	// the chan twice
	resultsCache := make(map[string]struct{})

	var match tagQueryExpression.TagStringMatcher
	if q.tagQuery != nil {
		match = q.tagQuery.GetMatcher()
	}

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
		tags, err := tagMapFromStrings(def.Tags)
		if err != nil {
			corruptIndex.Inc()
			log.Errorf("memory-idx: ID %q has tags %+v with invalid format", id, def.Tags)
			continue
		}

		metricTags := make(map[string]struct{}, 0)
		for key := range tags {
			// this tag has already been pushed into tagCh, so we can stop evaluating
			if _, ok := resultsCache[key]; ok {
				continue
			}

			if match != nil {
				// the value doesn't match the requirements
				if !match(key) {
					continue
				}
			}

			// keeping that value as it satisfies all conditions
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
}

// RunGetTags executes the tag query and returns all the tags of the
// resulting metrics
func (q *TagQuery) RunGetTags() map[string]struct{} {
	maxTagCount := int32(math.MaxInt32)
	matchName := true
	q.sortByCost()

	workersWg := sync.WaitGroup{}
	if q.tagQuery != nil {
		workersWg.Add(1)
		// start a thread to calculate the maximum possible number of tags.
		// this might not always complete before the query execution, but in most
		// cases it likely will. when it does end before the execution of the query,
		// the value of maxTagCount will be used to abort the query execution once
		// the max number of possible tags has been reached
		go atomic.StoreInt32(&maxTagCount, int32(q.getMaxTagCount(&workersWg)))

		// we know there can only be 1 tag filter, so if we detect that the given
		// tag condition matches the special tag "name", we can omit the filtering
		// because every metric has a name.
		matchName = q.tagQuery.GetMatcher()("name")
	}

	prepareFiltersWg := sync.WaitGroup{}
	prepareFiltersWg.Add(1)
	go func() {
		defer prepareFiltersWg.Done()
		q.prepareFilters()
	}()

	initialIds := make(chan schema.MKey, 1000)
	stopCh := q.getInitialIds(&workersWg, initialIds)
	tagCh := make(chan string)

	prepareFiltersWg.Wait()

	// start the tag query workers. they'll consume the ids on the idCh and
	// evaluate for each of them whether it satisfies all the conditions
	// defined in the query expressions. then they will extract the tags of
	// those that satisfy all conditions and push them into tagCh.
	workersWg.Add(TagQueryWorkers)
	for i := 0; i < TagQueryWorkers; i++ {
		go q.filterTagsFromChan(&workersWg, initialIds, tagCh, stopCh, matchName)
	}

	go func() {
		workersWg.Wait()
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

	workersWg.Wait()
	return result
}
