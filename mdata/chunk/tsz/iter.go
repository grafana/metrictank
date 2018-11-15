package tsz

type Iter interface {
	Next() bool
	Values() (uint32, float64)
	Err() error
}
