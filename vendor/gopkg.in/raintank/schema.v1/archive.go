package schema

import (
	"strconv"
)

//go:generate stringer -type=Method -linecomment

// Archive represents a metric archive
// the zero value represents a raw metric
// any non-zero value represents a certain
// aggregation method (lower bits) and
// aggregation span (higher bits)
type Archive uint8

func NewArchive(method Method, span uint32) Archive {
	code := spanHumanToCode[span]
	return Archive(uint8(method) | code<<4)
}

// String returns the traditional key suffix like sum_600 etc
// (invalid to call this for raw archives)
func (a Archive) String() string {
	method := Method(a & 0x0F)
	span := uint8(a >> 4)
	return method.String() + "_" + strconv.FormatInt(int64(spanCodeToHuman[span]), 10)
}

type Method uint8

const (
	Avg Method = iota + 1 // avg
	Sum                   // sum
	Lst                   // lst
	Max                   // max
	Min                   // min
	Cnt                   // cnt
)

// input will be like 600 -> to optimized form
// but also optimized to human friendly

var spanHumanToCode map[uint32]uint8
var spanCodeToHuman map[uint8]uint32

func init() {
	spans := []uint32{2, 5, 10, 15, 30, 60, 90, 120, 150, 300, 600, 900, 1200, 1800, 45 * 60, 3600, 3600 + 30*60, 2 * 3600, 3 * 3600, 4 * 3600, 5 * 3600, 6 * 3600}

	spanHumanToCode = make(map[uint32]uint8)
	spanCodeToHuman = make(map[uint8]uint32)

	for i, human := range spans {
		code := uint8(i)
		spanHumanToCode[human] = code
		spanCodeToHuman[code] = human
	}

}
