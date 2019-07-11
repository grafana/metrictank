package conf

import "math/bits"

type Method int

const (
	Avg Method = iota + 1
	Sum
	Lst
	Max
	Min
)

// NumSeries returs how many series will be created for the given methods
func NumSeries(methods []Method) int {

	// we set the following bits for when the corresponding serie will be generated
	// 0 count
	// 1 sum
	// 2 lst
	// 3 max
	// 4 min
	var buf uint8

	for _, method := range methods {
		switch method {
		case Avg:
			buf |= 1
			buf |= 2
		case Sum:
			buf |= 2
		case Lst:
			buf |= 4
		case Max:
			buf |= 8
		case Min:
			buf |= 16
		}
	}
	return bits.OnesCount8(buf)
}
