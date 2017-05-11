package expr

import "github.com/raintank/metrictank/api/models"

type Func interface {
	// Signature declares input and output arguments (return values)
	// input args can be optional in which case they can be specified positionally or via keys if you want to specify params that come after un-specified optional params
	// NewPlan() will only create the plan of the expressions it parsed correspond to the signatures provided by the function
	Signature() ([]Arg, []Arg)
	// NeedRange allows a func to express that to be able to return data in the given from to, it will need input data in the returned from-to window.
	// (e.g. movingAverage of 5min needs data as of from-5min)
	NeedRange(from, to uint32) (uint32, uint32)
	// Exec executes the function
	Exec(map[Req][]models.Series) ([]models.Series, error)
}

type funcConstructor func() Func

type funcDef struct {
	constr funcConstructor
	stable bool
}

var funcs map[string]funcDef

func init() {
	// keys must be sorted alphabetically. but functions with aliases can go together, in which case they are sorted by the first of their aliases
	funcs = map[string]funcDef{
		"alias":          {NewAlias, true},
		"aliasByNode":    {NewAliasByNode, true},
		"avg":            {NewAvgSeries, true},
		"averageSeries":  {NewAvgSeries, true},
		"consolidateBy":  {NewConsolidateBy, true},
		"movingAverage":  {NewMovingAverage, false},
		"perSecond":      {NewPerSecond, true},
		"smartSummarize": {NewSmartSummarize, false},
		"sum":            {NewSumSeries, true},
		"sumSeries":      {NewSumSeries, true},
		"transformNull":  {NewTransformNull, true},
	}
}
