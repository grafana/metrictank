package expr

import (
	"errors"
	"fmt"

	"github.com/raintank/metrictank/api/models"
)

type Req struct {
	Query string
	From  uint32 // from for this particular pattern
	To    uint32 // to for this particular pattern
}

type Plan struct {
	Reqs          []Req
	exprs         []*expr
	MaxDataPoints uint32
	From          uint32 // global request scoped from
	To            uint32 // global request scoped to
}

// Plan validates the expression and comes up with the initial (potentially non-optimal) execution plan
// traverse tree and as we go down:
// make sure function exists
// * tentative validation pre function call,
// * let function validate input arguments further
// * allow functions to extend the notion of which data is required
// * future version: allow functions to mark safe to pre-aggregate using consolidateBy or not
func NewPlan(exprs []*expr, from, to, mdp uint32, stable bool, reqs []Req) (Plan, error) {
	var err error
	for _, e := range exprs {
		reqs, err = newplan(e, from, to, stable, reqs)
		if err != nil {
			return Plan{}, err
		}
	}
	return Plan{
		Reqs:          reqs,
		exprs:         exprs,
		MaxDataPoints: mdp,
		From:          from,
		To:            to,
	}, nil
}

func newplan(e *expr, from, to uint32, stable bool, reqs []Req) ([]Req, error) {
	fmt.Println("HUH PLAN GOT", e)
	if e.etype != etFunc && e.etype != etName {
		return nil, errors.New("request must be a function call or metric pattern")
	}
	if e.etype == etName {
		reqs = append(reqs, Req{
			e.target,
			from,
			to,
		})
		return reqs, nil
	}

	// here e.type is guaranteed to be etFunc
	fdef, ok := funcs[e.target]
	if !ok {
		return nil, ErrUnknownFunction(e.target)
	}
	if stable && !fdef.stable {
		return nil, ErrUnknownFunction(e.target)
	}

	fn := fdef.constr()

	args, _ := fn.Signature()
	if len(e.args) != len(args) {
		return nil, ErrArgumentBadType
	}
	for i, argGot := range e.args {
		// we can't do extensive, accurate validation here because what the output from a function we depend on
		// might be dynamically typed.
		argExp := args[i]
		switch argExp {
		case series:
			if argGot.etype != etName && argGot.etype != etFunc {
				return nil, ErrArgumentBadType
			}
		case seriesList:
			if argGot.etype != etName && argGot.etype != etFunc {
				return nil, ErrArgumentBadType
			}
		case integer:
			if argGot.etype != etConst {
				return nil, ErrArgumentBadType
			}
		case float:
			if argGot.etype != etConst {
				return nil, ErrArgumentBadType
			}
		case str:
			if argGot.etype != etString {
				return nil, ErrArgumentBadType
			}
		}
	}
	err := fn.Init(e.args)
	if err != nil {
		return nil, err
	}
	from, to = fn.Depends(from, to)
	// look at which arguments are requested
	// if the args are series, they are to be requested with the potentially extended to/from
	// if they are not, keep traversing the tree until we find out which metrics to fetch and for which durations
	for _, arg := range e.args {
		if arg.etype == etName || arg.etype == etFunc {
			reqs, err = newplan(arg, from, to, stable, reqs)
			if err != nil {
				return nil, err
			}
		}
	}
	return reqs, nil
}

func (p Plan) Run(data map[Req][]models.Series) ([]models.Series, error) {
	var out []models.Series
	for _, expr := range p.exprs {
		o, err := p.run(p.From, p.To, data, expr)
		if err != nil {
			return nil, err
		}
		out = append(out, o...)
	}
	return out, nil
}

func (p Plan) run(from, to uint32, data map[Req][]models.Series, e *expr) ([]models.Series, error) {
	if e.etype != etFunc && e.etype != etName {
		panic("this should never happen. request must be a function call or metric pattern")
	}
	if e.etype == etName {
		req := Req{
			e.target,
			from,
			to,
		}
		return data[req], nil
	}

	// here e.type is guaranteed to be etFunc
	fdef, ok := funcs[e.target]
	if !ok {
		// this case should never happen since should have been validated before
		panic(ErrUnknownFunction(e.target))
	}
	fn := fdef.constr()
	err := fn.Init(e.args)
	if err != nil {
		return nil, err
	}
	from, to = fn.Depends(from, to)
	// look at which arguments are requested
	// if the args are series, they are to be requested with the potentially extended to/from
	// if they are not, keep traversing the tree until we find out which metrics to fetch and for which durations
	results := make([]interface{}, len(e.args))
	for i, arg := range e.args {
		if arg.etype == etName || arg.etype == etFunc {
			result, err := p.run(from, to, data, arg)
			if err != nil {
				return nil, err
			}
			results[i] = result
		}
	}
	// we now have all our args and can process the data and return
	rets, err := fn.Exec(results...)
	if err != nil {
		return nil, err
	}
	series := make([]models.Series, len(rets))
	for i, ret := range rets {
		series[i] = ret.(models.Series)
	}
	return series, nil
}
