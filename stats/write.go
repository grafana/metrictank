package stats

import (
	"strconv"
	"time"
)

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

func WriteUint32(buf, prefix, name, suffix, tags[]byte, val uint32, now time.Time) []byte {
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
