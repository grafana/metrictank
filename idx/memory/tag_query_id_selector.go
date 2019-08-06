package memory

import (
	"sync"

	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/raintank/schema"
)

// idSelector looks up ids from the metric & meta tag index according to
// a given tagquery expression.
// it is used to build the initial query set when running a tag query,
// this result set may later be filtered down by other expressions.
type idSelector struct {
	ctx    *TagQueryContext
	expr   tagquery.Expression
	idChWg sync.WaitGroup
	idCh   chan schema.MKey
	stopCh chan struct{}
}

// newIdSelector initializes an id selector based on the given arguments.
// each id selector instance is only intended for being used a single time,
// reusing it is not intended
func newIdSelector(expr tagquery.Expression, ctx *TagQueryContext) *idSelector {
	return &idSelector{
		ctx:    ctx,
		expr:   expr,
		idCh:   make(chan schema.MKey),
		stopCh: make(chan struct{}),
	}
}

// getIds asynchronously looks up all ID's of the initial result set
// It returns:
// a channel through which the IDs of the initial result set will be sent
// a stop channel, which when closed, will cause the lookup jobs to be aborted
// this is the only method of idSelector which shall ever be called by users,
// all other methods of this type are only helpers of getIds
func (i *idSelector) getIds() (chan schema.MKey, chan struct{}) {
	// this wait group is used to wait for all id producing go routines to complete
	// their respective jobs, once they're done we can close the id chan
	// we initially set it to 2 because there will be at least 1 routine to look up
	// ids from the metric index and 1 to check the meta tag index. when looking up
	// from the meta tag index this waitgroup may temporarily get further increased
	i.idChWg.Add(2)
	go func() {
		i.idChWg.Wait()
		close(i.idCh)
	}()

	if i.expr.OperatesOnTag() {
		i.byTag()
	} else {
		i.byTagValue()
	}

	return i.idCh, i.stopCh
}

// byTagValue looks up all ids matching the expression i.expr and pushes them into
// the id chan.
// it assumes that expression i.expr operates on tag values
func (i *idSelector) byTagValue() {
	go i.byTagValueFromMetricTagIndex()

	// if this is a sub query we want to ignore the meta tag index,
	// otherwise we'd risk to create a loop of sub queries creating
	// each other
	// same when meta tag support is disabled in the config
	if !tagquery.MetaTagSupport || i.ctx.subQuery {
		i.idChWg.Done()
		return
	}

	go i.byTagValueFromMetaTagIndex()
}

// byTagValueFromMetricTagIndex looks up all ids matching the expression i.expr
// from the metric index, it then pushes all of them into the id chan.
// this method assumes that the expression i.expr operates on tag values
func (i *idSelector) byTagValueFromMetricTagIndex() {
	defer i.idChWg.Done()

	// if expression value matches exactly we can directly look up the ids by it as key.
	// this is faster than having to call expr.Matches on each value
	if i.expr.MatchesExactly() {
		for id := range i.ctx.index[i.expr.GetKey()][i.expr.GetValue()] {
			select {
			case <-i.stopCh:
				return
			case i.idCh <- id:
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
			select {
			case <-i.stopCh:
				return
			case i.idCh <- id:
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
	defer i.idChWg.Done()

	// if expression matches value exactly we can directly look up the ids by it as key.
	// this is faster than having to call expr.Matches on each value
	if i.expr.MatchesExactly() {
		for _, metaRecordId := range i.ctx.mti[i.expr.GetKey()][i.expr.GetValue()] {
			select {
			case <-i.stopCh:
				return
			default:
			}
			i.evaluateMetaRecord(metaRecordId)
		}
		return
	}

	for value, records := range i.ctx.mti[i.expr.GetKey()] {
		select {
		case <-i.stopCh:
			return
		default:
		}

		if !i.expr.Matches(value) {
			continue
		}

		for _, metaRecordId := range records {
			i.evaluateMetaRecord(metaRecordId)
		}
	}
}

// byTag looks up all ids matching the expression i.expr and pushes them into
// the id chan.
// it assumes that expression i.expr operates on tag keys
func (i *idSelector) byTag() {
	go i.byTagFromMetricTagIndex()

	// if this is a sub query we want to ignore the meta tag index,
	// otherwise we'd risk to create a loop of sub queries creating
	// each other
	// same when meta tag support is disabled in the config
	if !tagquery.MetaTagSupport || i.ctx.subQuery {
		i.idChWg.Done()
		return
	}

	go i.byTagFromMetaTagIndex()
}

// byTagFromMetricTagIndex looks up all ids matching the expression i.expr
// from the metric index, it then pushes all of them into the id chan.
// this method assumes that the expression i.expr operates on tag keys
func (i *idSelector) byTagFromMetricTagIndex() {
	defer i.idChWg.Done()

	if i.expr.MatchesExactly() {
		for _, ids := range i.ctx.index[i.expr.GetKey()] {
			for id := range ids {
				select {
				case <-i.stopCh:
					break
				case i.idCh <- id:
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
				select {
				case <-i.stopCh:
					return
				case i.idCh <- id:
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
	defer i.idChWg.Done()

	if i.expr.MatchesExactly() {
		for _, records := range i.ctx.mti[i.expr.GetKey()] {
			for _, metaRecordId := range records {
				i.evaluateMetaRecord(metaRecordId)
			}
		}

		return
	}

	for tag := range i.ctx.mti {
		if !i.expr.Matches(tag) {
			continue
		}

		for _, records := range i.ctx.mti[tag] {
			for _, metaRecordId := range records {
				i.evaluateMetaRecord(metaRecordId)
			}
		}
	}
}

// evaluateMetaRecord takes a meta record id, it then looks up the corresponding
// meta record, builds a sub query from its expressions and executes the sub query
func (i *idSelector) evaluateMetaRecord(id recordId) {
	record, ok := i.ctx.metaRecords[id]
	if !ok {
		corruptIndex.Inc()
		return
	}

	query, err := i.subQueryFromExpressions(record.Expressions)
	if err != nil {
		return
	}

	i.idChWg.Add(1)
	go i.runSubQuery(query)
}

// subQueryFromExpressions takes a set of expressions and instantiates a new
// sub query based on them.
// it is used as a helper to lookup ids matching the expressions associated
// with a meta tag
func (i *idSelector) subQueryFromExpressions(expressions tagquery.Expressions) (TagQueryContext, error) {
	var queryCtx TagQueryContext

	query, err := tagquery.NewQuery(expressions, i.ctx.query.From)
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
// it reads the returned results and pushes them into the id chan
func (i *idSelector) runSubQuery(query TagQueryContext) {
	defer i.idChWg.Done()

	resCh := query.Run(i.ctx.index, i.ctx.byId, i.ctx.mti, i.ctx.metaRecords)

	for id := range resCh {
		select {
		// abort if query has been stopped
		case <-i.stopCh:
			return
		case i.idCh <- id:
		}
	}
}
