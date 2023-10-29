package stats

import (
	"strconv"
	"time"
)

// Write* functions append a graphite metric line to the given buffer.
// `buf` is the incoming buffer to be appended to
// `prefix` is an optional prefix to the metric name which must have a trailing '.' if present
// `name` is the required name of the metric. It should not have a leading or trailing '.' or a trailing ';'
// `suffix` is an optional suffix to the metric name which must have a leading '.' if present.  It should not have a trailing ';'
// `tags` is an optional list of tags which must have a leading ';' if present.
// `val` is the value of the metric
// `now` is the time that the metrics should be reported at
// returns `buf` with the new metric line appended
func WriteFloat64(buf, prefix, name, suffix, tags []byte, val float64, now time.Time) []byte {
	buf = append(buf, prefix...)
	buf = append(buf, name...)
	buf = append(buf, suffix...)
	buf = append(buf, tags...)
	buf = append(buf, ' ')
	buf = strconv.AppendFloat(buf, val, 'f', -1, 64)
	buf = append(buf, ' ')
	buf = strconv.AppendInt(buf, now.Unix(), 10)
	return append(buf, '\n')
}

func WriteUint32(buf, prefix, name, suffix, tags []byte, val uint32, now time.Time) []byte {
	buf = append(buf, prefix...)
	buf = append(buf, name...)
	buf = append(buf, suffix...)
	buf = append(buf, tags...)
	buf = append(buf, ' ')
	buf = strconv.AppendUint(buf, uint64(val), 10)
	buf = append(buf, ' ')
	buf = strconv.AppendInt(buf, now.Unix(), 10)
	return append(buf, '\n')
}

func WriteUint64(buf, prefix, name, suffix, tags []byte, val uint64, now time.Time) []byte {
	buf = append(buf, prefix...)
	buf = append(buf, name...)
	buf = append(buf, suffix...)
	buf = append(buf, tags...)
	buf = append(buf, ' ')
	buf = strconv.AppendUint(buf, val, 10)
	buf = append(buf, ' ')
	buf = strconv.AppendInt(buf, now.Unix(), 10)
	return append(buf, '\n')
}

func WriteInt32(buf, prefix, name, suffix, tags []byte, val int32, now time.Time) []byte {
	buf = append(buf, prefix...)
	buf = append(buf, name...)
	buf = append(buf, suffix...)
	buf = append(buf, tags...)
	buf = append(buf, ' ')
	buf = strconv.AppendInt(buf, int64(val), 10)
	buf = append(buf, ' ')
	buf = strconv.AppendInt(buf, now.Unix(), 10)
	return append(buf, '\n')
}
