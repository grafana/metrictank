package memory

import (
	"sync"

	"github.com/grafana/metrictank/pkg/expr/tagquery"
	"github.com/grafana/metrictank/pkg/schema"
)

// idSelector looks up ids from the metric & meta tag index according to
// a given tagquery expression.
// it is used to build the initial query set when running a tag query,
// this result set may later be filtered down by other expressions.
type idSelector struct {
	ctx      *TagQueryContext
	expr     tagquery.Expression
	rawResCh chan schema.MKey
	workerWg sync.WaitGroup
	stopCh   chan struct{}
	concGate chan struct{}
}

// newIdSelector initializes an id selector based on the given arguments.
// each id selector instance is only intended for being used a single time,
// reusing it is not intended
func newIdSelector(expr tagquery.Expression, ctx *TagQueryContext) *idSelector {
	return &idSelector{
		ctx:      ctx,
		expr:     expr,
		stopCh:   make(chan struct{}),
		concGate: make(chan struct{}, TagQueryWorkers), // gates concurrency
	}
}

// getIds asynchronously looks up all ID's of the initial result set
// It returns:
// a channel through which the IDs of the initial result set will be sent
// a stop channel, which when closed, will cause the lookup jobs to be aborted
// this is the only method of idSelector which shall ever be called by users,
// all other methods of this type are only helpers of getIds
func (i *idSelector) getIds(resCh chan schema.MKey, stopCh chan struct{}) {
	if stopCh != nil {
		i.stopCh = stopCh
	}

	// this wait group is used to wait for all id producing go routines to complete
	// their respective jobs, once they're done we can close the id chan
	// we initially set it to 2 because there will be at least 1 routine to look up
	// ids from the metric index and 1 to check the meta tag index. when looking up
	// from the meta tag index this waitgroup may temporarily get further increased
	i.workerWg.Add(2)

	// if meta tag support is enabled and we create subqueries out of looked up meta
	// records, then its possible that we will end up with duplicate results. to
	// prevent this we spawn a separate worker process which deduplicates them
	deduplicateResults := i.ctx.useMetaTagIndex()

	var dedupWg sync.WaitGroup
	if deduplicateResults {
		i.rawResCh = make(chan schema.MKey)
		dedupWg.Add(1)
		go i.deduplicateRawResults(&dedupWg, resCh)
	} else {
		i.rawResCh = resCh
	}

	if i.expr.OperatesOnTag() {
		i.byTag()
	} else {
		i.byTagValue()
	}

	i.workerWg.Wait()
	dedupWg.Wait()
}

// deduplicateRawResults reads the channel i.rawResCh and deduplicates all the ids
// in it, then it inserts the unique ids into the channel i.resCh. This is only
// necessary for queries involving meta tag looksup, without meta tag lookups its
// not possible that duplicate ids will end up in i.rawResCh
func (i *idSelector) deduplicateRawResults(dedupWg *sync.WaitGroup, resCh chan schema.MKey) {
	// once all workers are finished we want to close the raw result chan
	// to make this routine exit
	go func() {
		i.workerWg.Wait()
		close(i.rawResCh)
	}()

	defer dedupWg.Done()
	seen := make(map[schema.MKey]struct{})
	for id := range i.rawResCh {
		if _, ok := seen[id]; ok {
			continue
		}

		resCh <- id
		seen[id] = struct{}{}
	}
}

// byTagValue looks up all ids matching the expression i.expr and pushes them into
// the id chan.
// it assumes that expression i.expr operates on tag values
func (i *idSelector) byTagValue() {
	go i.byTagValueFromMetricTagIndex()

	if !i.ctx.useMetaTagIndex() {
		i.workerWg.Done()
		return
	}

	go i.byTagValueFromMetaTagIndex()
}

// byTagValueFromMetricTagIndex looks up all ids matching the expression i.expr
// from the metric index, it then pushes all of them into the id chan.
// this method assumes that the expression i.expr operates on tag values
func (i *idSelector) byTagValueFromMetricTagIndex() {
	defer i.workerWg.Done()

	// if expression value matches exactly we can directly look up the ids by it as key.
	// this is faster than having to call expr.Matches on each value
	if i.expr.MatchesExactly() {
		for id := range i.ctx.index[i.expr.GetKey()][i.expr.GetValue()] {
			if !i.ctx.withinTimeBounds(id) {
				continue
			}

			select {
			case <-i.stopCh:
				return
			case i.rawResCh <- id:
			}
		}

		return
	}

	// look up all values of the given key and check for each of them if it
	// matches the expression.
	// if there's a match, push all ids of the value into the id chan
	for value, ids := range i.ctx.index[i.expr.GetKey()] {
		if !i.expr.Matches(value) {
			continue
		}

		for id := range ids {
			if !i.ctx.withinTimeBounds(id) {
				continue
			}

			select {
			case <-i.stopCh:
				return
			case i.rawResCh <- id:
			}
		}
	}
}

// byTagValueFromMetaTagIndex looks up all ids matching the expression i.expr
// from the meta tag index, it then pushes all of them into the id chan.
// this method assumes that the expression i.expr operates on tag values.
// this function creates sub-queries based on the expressions associated with the
// meta tags which match i.expr, it then merges all results of the subqueries
func (i *idSelector) byTagValueFromMetaTagIndex() {
	defer i.workerWg.Done()

	for _, recordId := range i.ctx.metaTagIndex.getByTagValue(i.expr, false) {
		select {
		case <-i.stopCh:
			return
		default:
		}
		i.evaluateMetaRecord(recordId)
	}
}

// byTag looks up all ids matching the expression i.expr and pushes them into
// the id chan.
// it assumes that expression i.expr operates on tag keys
func (i *idSelector) byTag() {
	go i.byTagFromMetricTagIndex()

	if !i.ctx.useMetaTagIndex() {
		i.workerWg.Done()
		return
	}

	go i.byTagFromMetaTagIndex()
}

// byTagFromMetricTagIndex looks up all ids matching the expression i.expr
// from the metric index, it then pushes all of them into the id chan.
// this method assumes that the expression i.expr operates on tag keys
func (i *idSelector) byTagFromMetricTagIndex() {
	defer i.workerWg.Done()

	if i.expr.MatchesExactly() {
		for _, ids := range i.ctx.index[i.expr.GetKey()] {
			for id := range ids {
				if !i.ctx.withinTimeBounds(id) {
					continue
				}

				select {
				case <-i.stopCh:
					return
				case i.rawResCh <- id:
				}
			}
		}

		return
	}

	for tag := range i.ctx.index {
		if !i.expr.Matches(tag) {
			continue
		}

		for _, ids := range i.ctx.index[tag] {
			for id := range ids {
				if !i.ctx.withinTimeBounds(id) {
					continue
				}

				select {
				case <-i.stopCh:
					return
				case i.rawResCh <- id:
				}
			}
		}
	}
}

// byTagFromMetaTagIndex looks up all ids matching the expression i.expr
// from the meta tag index, it then pushes all of them into the id chan.
// this method assumes that the expression i.expr operates on tag keys.
// this function creates sub-queries based on the expressions associated with the
// meta tags which match i.expr, it then merges all results of the subqueries
func (i *idSelector) byTagFromMetaTagIndex() {
	defer i.workerWg.Done()

	for _, recordId := range i.ctx.metaTagIndex.getByTag(i.expr, false) {
		select {
		case <-i.stopCh:
			return
		default:
		}
		i.evaluateMetaRecord(recordId)
	}
}

// evaluateMetaRecord takes a meta record id, it then looks up the corresponding
// meta record, builds a sub query from its expressions and executes the sub query
func (i *idSelector) evaluateMetaRecord(id recordId) {
	record, ok := i.ctx.metaTagRecords.getMetaRecordById(id)
	if !ok {
		return
	}

	query, err := i.subQueryFromExpressions(record.Expressions)
	if err != nil {
		corruptIndex.Inc()
		return
	}

	i.concGate <- struct{}{}
	i.workerWg.Add(1)
	go i.runSubQuery(query)
}

// subQueryFromExpressions takes a set of expressions and instantiates a new
// sub query based on them.
// it is used as a helper to lookup ids matching the expressions associated
// with a meta tag
func (i *idSelector) subQueryFromExpressions(expressions tagquery.Expressions) (TagQueryContext, error) {
	var queryCtx TagQueryContext

	query, err := tagquery.NewQuery(expressions, i.ctx.query.From, 0)
	if err != nil {
		// this means we've stored a meta record containing invalid queries
		corruptIndex.Inc()
		return queryCtx, err
	}

	queryCtx = NewTagQueryContext(query)
	queryCtx.subQuery = true

	return queryCtx, nil
}

// runSubQuery takes a sub-query and executes it.
// it passes the rawResCh into the sub query, so the query results get
// directly pushed into it
func (i *idSelector) runSubQuery(query TagQueryContext) {
	defer i.workerWg.Done()
	query.Run(i.ctx.index, i.ctx.byId, i.ctx.metaTagIndex, i.ctx.metaTagRecords, i.rawResCh)
	<-i.concGate
}
