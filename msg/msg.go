package msg

import schema "gopkg.in/raintank/schema.v1"

// Point represents an incoming datapoint
// represented either as MetricData or MetricPointId2
type Point struct {
	Val   uint8 // 0 means use md, 1 means use point
	Md    *schema.MetricData
	Point schema.MetricPointId2
}
