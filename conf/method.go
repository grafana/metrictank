package conf

type Method int

const (
	Avg Method = iota + 1
	Sum
	Lst
	Max
	Min
)
