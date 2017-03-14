package expr

import (
	"errors"
	"fmt"
)

type Req struct {
	Query string
	From  uint32
	To    uint32
}

type Plan struct {
	Reqs          []Req
	exprs         []*expr
	MaxDataPoints uint32
}

// Plan validates the expression and comes up with the initial (potentially non-optimal) execution plan
// traverse tree and as we go down:
// make sure function exists
// * tentative validation pre function call,
// * let function validate input arguments further
// * allow functions to extend the notion of which data is required
// * future version: allow functions to mark safe to pre-aggregate using consolidateBy or not
func NewPlan(exprs []*expr, from, to, mdp uint32, reqs []Req) (Plan, error) {
	var err error
	for _, e := range exprs {
		reqs, err = newplan(e, from, to, reqs)
		if err != nil {
			return Plan{}, err
		}
	}
	return Plan{
		Reqs:          reqs,
		exprs:         exprs,
		MaxDataPoints: mdp,
	}, nil
}

func newplan(e *expr, from, to uint32, reqs []Req) ([]Req, error) {
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
		return nil, errUnknownFunction(e.target)
	}
	fn := fdef.constr()

	args, _ := fn.Signature()
	if len(e.args) != len(args) {
		return nil, ErrArgumentBadType
	}
	for i, argGot := range e.args {
		argExp := e.args[i]
		if argGot.etype != argExp.etype {
			return nil, ErrArgumentBadType
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
			reqs, err = newplan(arg, from, to, reqs)
			if err != nil {
				return nil, err
			}
		}
	}
	return reqs, nil
}
