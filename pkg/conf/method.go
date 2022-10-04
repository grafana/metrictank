package conf

import "fmt"

type Method int

const (
	Avg Method = iota + 1
	Sum
	Lst
	Max
	Min
)

func NewMethod(str string) (Method, error) {
	switch str {
	case "average", "avg":
		return Avg, nil
	case "sum":
		return Sum, nil
	case "last":
		return Lst, nil
	case "max":
		return Max, nil
	case "min":
		return Min, nil
	}
	return 0, fmt.Errorf("unknown aggregation method %q", str)
}

func (m Method) String() string {
	switch m {
	case Avg:
		return "avg"
	case Sum:
		return "sum"
	case Lst:
		return "last"
	case Max:
		return "max"
	case Min:
		return "min"
	}
	return fmt.Sprintf("Method(%d)", m)
}
