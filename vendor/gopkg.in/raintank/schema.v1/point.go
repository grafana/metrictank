package schema

//go:generate msgp
type Point struct {
	Val float64
	Ts  uint32
}
