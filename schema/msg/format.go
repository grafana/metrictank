package msg

//go:generate stringer -type=Format

type Format uint8

// incoming messages in the mdm topic come in 3 flavors, and are mostly defined by the below constants

// 1) MetricPoint observations and array variants
// <FormatMetricPointWithoutOrg><28 more bytes>
// <FormatMetricPoint><32 more bytes>
// <FormatMetricPointAray><FormatMetricPoint|FormatMetricPointWithoutOrg><28 or more bytes>
//
// 2) index control messages
//
// <FormatIndexControlMessage><any other bytes>

// 3) MetricData
//
// anything that has a first byte not matching any of the predefined constants.
// in practice, the first byte of a MetricData will be 0x89,
// though experts writing their own publishers and encoders may have found ways to
// use bytes 0x85 through 0x89 as first byte, though this seems unlikely and discouraged
// see https://github.com/grafana/metrictank/issues/2028
// identifier of message format
const (
	FormatMetricDataArrayJson Format = iota
	FormatMetricDataArrayMsgp
	FormatMetricPoint
	FormatMetricPointWithoutOrg
	FormatIndexControlMessage
	FormatMetricPointArray
)
