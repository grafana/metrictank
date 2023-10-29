package expr

import (
	"fmt"
	"io"
	"strings"

	"github.com/grafana/metrictank/internal/consolidation"
	"github.com/grafana/metrictank/internal/schema"
	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/grafana/metrictank/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type Optimizations struct {
	PreNormalization bool
	MDP              bool
}

func (o Optimizations) ApplyUserPrefs(s string) (Optimizations, error) {
	// no user override. stick to what we have
	if s == "" {
		return o, nil
	}
	// user passed an override. it's either 'none' (no optimizations) or a list of the ones that should be enabled
	o.PreNormalization = false
	o.MDP = false
	if s == "none" {
		return o, nil
	}
	prefs := strings.Split(s, ",")
	for _, pref := range prefs {
		switch pref {
		case "pn":
			o.PreNormalization = true
		case "mdp":
			o.MDP = true
		default:
			return o, fmt.Errorf("unrecognized optimization %q", pref)
		}
	}
	return o, nil
}

// Req represents a request for one/more series
type Req struct {
	Query   string // whatever was parsed as the query out of a graphite target. e.g. target=sum(foo.{b,a}r.*) -> foo.{b,a}r.* -> this will go straight to index lookup
	From    uint32
	To      uint32
	Cons    consolidation.Consolidator // can be 0 to mean undefined
	PNGroup models.PNGroup
	MDP     uint32 // if we can MDP-optimize, reflects runtime consolidation MaxDataPoints. 0 otherwise.
}

// NewReq creates a new Req. pass cons=0 to leave consolidator undefined,
// leaving up to the caller (in graphite's case, it would cause a lookup into storage-aggregation.conf)
func NewReq(query string, from, to uint32, cons consolidation.Consolidator, PNGroup models.PNGroup, MDP uint32) Req {
	return Req{
		Query:   query,
		From:    from,
		To:      to,
		Cons:    cons,
		PNGroup: PNGroup,
		MDP:     MDP,
	}
}

func NewReqFromContext(query string, c Context) Req {
	r := Req{
		Query: query,
		From:  c.from,
		To:    c.to,
		Cons:  c.consol,
	}
	if c.optimizations.PreNormalization {
		r.PNGroup = c.PNGroup
	}
	if c.optimizations.MDP {
		r.MDP = c.MDP
	}
	return r
}

// NewReqFromSeries generates a Req back from a series
// a models.Series has all the properties attached to it
// to find out which Req it came from
func NewReqFromSerie(serie models.Series) Req {
	return Req{
		Query:   serie.QueryPatt,
		From:    serie.QueryFrom,
		To:      serie.QueryTo,
		Cons:    serie.QueryCons,
		PNGroup: serie.QueryPNGroup,
		MDP:     serie.QueryMDP,
	}

}

func (r Req) ToModel() models.Req {
	return models.Req{
		Pattern:   r.Query,
		From:      r.From,
		To:        r.To,
		MaxPoints: r.MDP,
		PNGroup:   r.PNGroup,
		ConsReq:   r.Cons,
	}
}

type Plan struct {
	Reqs          []Req          // data that needs to be fetched before functions can be executed
	funcs         []GraphiteFunc // top-level funcs to execute, the head of each tree for each target
	exprs         []*expr
	MaxDataPoints uint32
	From          uint32  // global request scoped from
	To            uint32  // global request scoped to
	dataMap       DataMap // set via Run()
}

func (p Plan) Dump(w io.Writer) {
	fmt.Fprintf(w, "Plan:\n")
	fmt.Fprintf(w, "* Exprs:\n")
	for _, e := range p.exprs {
		fmt.Fprintln(w, e.Print(2))
	}
	fmt.Fprintf(w, "* Reqs:\n")

	maxQueryLen := 5
	for _, r := range p.Reqs {
		if len(r.Query) > maxQueryLen {
			maxQueryLen = len(r.Query)
		}
	}
	// ! PNGroups are pointers which can be upto 21 characters long on 64bit
	headPatt := fmt.Sprintf("%%%ds %%12s %%12s %%25s %%21s %%6s\n", maxQueryLen)
	linePatt := fmt.Sprintf("%%%ds %%12d %%12d %%25s %%21d %%6d\n", maxQueryLen)
	fmt.Fprintf(w, headPatt, "query", "from", "to", "consolidator", "PNGroup", "MDP")

	for _, r := range p.Reqs {
		fmt.Fprintf(w, linePatt, r.Query, r.From, r.To, r.Cons, r.PNGroup, r.MDP)
	}
	fmt.Fprintf(w, "MaxDataPoints: %d\n", p.MaxDataPoints)
	fmt.Fprintf(w, "From: %d\n", p.From)
	fmt.Fprintf(w, "To: %d\n", p.To)
}

// NewPlan validates the expressions and comes up with the initial (potentially non-optimal) execution plan
// which is just a list of requests and the expressions.
// traverse tree and as we go down:
// * make sure function exists
// * validation of arguments
// * allow functions to modify the Context (change data range or consolidation)
// * future version: allow functions to mark safe to pre-aggregate using consolidateBy or not
func NewPlan(exprs []*expr, from, to, mdp uint32, stable bool, optimizations Optimizations) (Plan, error) {
	plan := Plan{
		exprs:         exprs,
		MaxDataPoints: mdp,
		From:          from,
		To:            to,
	}
	for _, e := range exprs {
		context := Context{
			from:          from,
			to:            to,
			MDP:           mdp,
			PNGroup:       0, // making this explicit here for easy code grepping
			optimizations: optimizations,
		}
		fn, reqs, err := newplan(e, context, stable, plan.Reqs)
		if err != nil {
			return Plan{}, err
		}
		plan.Reqs = reqs
		plan.funcs = append(plan.funcs, fn)
	}
	return plan, nil
}

// newplan adds requests as needed for the given expr, resolving function calls as needed
func newplan(e *expr, context Context, stable bool, reqs []Req) (GraphiteFunc, []Req, error) {

	// suppress duplicate queries such as target=foo&target=foo
	// note that unless `pre-normalization = false`,
	// this cannot suppress duplicate reqs in these cases:
	// target=foo&target=sum(foo)      // reqs are different, one has a PNGroup set
	// target=sum(foo)&target=sum(foo) // reqs get different PNGroups
	// perhaps in the future we can improve on this and
	// deduplicate the largest common (sub)expressions

	addReqIfNew := func(req Req) {
		for _, r := range reqs {
			if r == req {
				return
			}
		}
		reqs = append(reqs, req)
	}

	if e.etype != etFunc && e.etype != etName {
		return nil, nil, errors.NewBadRequest("request must be a function call or metric pattern")
	}
	if e.etype == etName {
		req := NewReqFromContext(e.str, context)
		addReqIfNew(req)
		return NewGet(req), reqs, nil
	} else if e.etype == etFunc && e.str == "seriesByTag" {
		// `seriesByTag` function requires resolving expressions to series
		// (similar to path expressions handled above). Since we need the
		// arguments of seriesByTag to do the resolution, we store the function
		// string back into the Query member of a new request to be parsed later.
		// TODO - find a way to prevent this parse/encode/parse/encode loop
		expressionStr := "seriesByTag(" + e.argsStr + ")"
		req := NewReqFromContext(expressionStr, context)
		addReqIfNew(req)
		return NewGet(req), reqs, nil
	}
	// here e.type is guaranteed to be etFunc
	fdef, ok := funcs[e.str]
	if !ok {
		return nil, nil, ErrUnknownFunction(e.str)
	}
	if stable && !fdef.stable {
		return nil, nil, ErrUnknownFunction(e.str)
	}

	fn := fdef.constr()
	reqs, err := newplanFunc(e, fn, context, stable, reqs)
	return fn, reqs, err
}

// newplanFunc adds requests as needed for the given expr, and validates the function input
// provided you already know the expression is a function call to the given function
func newplanFunc(e *expr, fn GraphiteFunc, context Context, stable bool, reqs []Req) ([]Req, error) {
	// first comes the interesting task of validating the arguments as specified by the function,
	// against the arguments that were parsed.

	argsExp, _ := fn.Signature()
	var err error

	// note:
	// * signature may have seriesLists in it, which means one or more args of type seriesList
	//   so it's legal to have more e.args than signature args in that case.
	// * we can't do extensive, accurate validation of the type here because what the output from a function we depend on
	//   might be dynamically typed. e.g. movingAvg returns 1..N series depending on how many it got as input

	// first validate the mandatory args
	pos := 0    // e.args[pos]     : next given arg to process
	cutoff := 0 // argsExp[cutoff] : will be first optional arg (if any)
	var (
		argExp  Arg
		nextPos int
	)
	for cutoff, argExp = range argsExp {
		if argExp.Optional() {
			break
		}
		if len(e.args) <= pos {
			return nil, fmt.Errorf("can't plan function %q: %w", e.str, ErrMissingArg)
		}

		nextPos, err = e.consumeBasicArg(pos, argExp)
		if err != nil {
			return nil, fmt.Errorf("can't plan function %q, arg %d: %w", e.str, pos, err)
		}
		pos = nextPos
	}
	if !argExp.Optional() {
		cutoff++
	}

	// we stopped iterating the mandatory args.
	// any remaining args should be due to optional args otherwise there's too many
	// we also track here which keywords can also be used for the given optional args
	// so that those args should not be specified via their keys anymore.

	seenKwargs := make(map[string]struct{})
	for _, argOpt := range argsExp[cutoff:] {
		if len(e.args) <= pos {
			break // no more args specified. we're done.
		}
		nextPos, err = e.consumeBasicArg(pos, argOpt)
		if err != nil {
			return nil, fmt.Errorf("can't plan function %q, optional arg %d: %w", e.str, pos, err)
		}
		pos = nextPos
		seenKwargs[argOpt.Key()] = struct{}{}
	}
	if len(e.args) > pos {
		return nil, ErrTooManyArg
	}

	// for any provided keyword args, verify that they are what the function stipulated
	// and that they have not already been specified via their position
	for key := range e.namedArgs {
		_, ok := seenKwargs[key]
		if ok {
			return nil, fmt.Errorf("can't plan function %q: %w", e.str, ErrKwargSpecifiedTwice{key})
		}
		err = e.consumeKwarg(key, argsExp[cutoff:])
		if err != nil {
			return nil, fmt.Errorf("can't plan function %q, kwarg %q: %w", e.str, key, err)
		}
		seenKwargs[key] = struct{}{}
	}

	// functions now have their non-series input args set,
	// so they should now be able to specify any context alterations
	context = fn.Context(context)
	// now that we know the needed context for the data coming into
	// this function, we can set up the input arguments for the function
	// that are series
	pos = 0
	for _, argExp = range argsExp {
		if pos >= len(e.args) {
			break // no more args specified. we're done.
		}
		switch argExp.(type) {
		case ArgSeries, ArgSeriesList, ArgSeriesLists, ArgIn:
			nextPos, reqs, err = e.consumeSeriesArg(pos, argExp, context, stable, reqs)
			if err != nil {
				return nil, fmt.Errorf("can't plan function %q, series arg %d: %w", e.str, pos, err)
			}
			pos = nextPos
		default:
			pos++
		}
	}
	return reqs, err
}

// Run invokes all processing as specified in the plan (expressions, from/to) against the given datamap
func (p *Plan) Run(dataMap DataMap) ([]models.Series, error) {
	var out []models.Series
	p.dataMap = dataMap
	for _, fn := range p.funcs {
		series, err := fn.Exec(p.dataMap)
		if err != nil {
			return nil, err
		}
		out = append(out, series...)
	}

	// while 'out' contains copies of the series, the datapoints, meta and tags properties need COW
	// see devdocs/expr.md
	for i, o := range out {
		if p.MaxDataPoints != 0 && len(o.Datapoints) > int(p.MaxDataPoints) {
			// series may have been created by a function that didn't know which consolidation function to default to.
			// in the future maybe we can do more clever things here. e.g. perSecond maybe consolidate by max.
			if o.Consolidator == 0 {
				o.Consolidator = consolidation.Avg
			}
			pointsCopy := pointSlicePool.GetMin(len(o.Datapoints))
			pointsCopy = pointsCopy[:len(o.Datapoints)]
			copy(pointsCopy, o.Datapoints)
			out[i].Datapoints, out[i].Interval = consolidation.ConsolidateNudged(pointsCopy, o.Interval, p.MaxDataPoints, o.Consolidator)
			out[i].Meta = out[i].Meta.CopyWithChange(func(in models.SeriesMetaProperties) models.SeriesMetaProperties {
				in.AggNumRC = consolidation.AggEvery(uint32(len(o.Datapoints)), p.MaxDataPoints)
				in.ConsolidatorRC = o.Consolidator
				return in
			})
			dataMap.Add(Req{}, out[i])
		}
	}
	return out, nil
}

func (p Plan) Clean() {
	p.dataMap.Clean()
}

func (p Plan) CheckedClean(targets []string) bool {
	type ReqSeries struct {
		req    Req
		series models.Series
	}
	// Map of pointer to end of slice to the first series we found with that address
	addrs := make(map[*schema.Point]ReqSeries)
	for req, series := range p.dataMap {
		for _, serie := range series {
			dpCap := cap(serie.Datapoints)
			if dpCap > 0 {
				addr := &(serie.Datapoints[0:dpCap][dpCap-1])
				if val, ok := addrs[addr]; ok {
					log.Errorf("Found results sharing a slice: query = %v, req1 = %v, series = %v, req2 = %v, series = %v", targets, val.req, val.series, req, serie)
					// Don't want to spew errors so move on (don't clean this one)
					return false
				}
				addrs[addr] = ReqSeries{req, serie}
			}
		}
	}
	p.Clean()
	return true
}
