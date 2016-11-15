package util

import (
	"fmt"
	"time"
)

func TS(ts interface{}) string {
	switch t := ts.(type) {
	case int64:
		return time.Unix(t, 0).Format("02 15:04:05")
	case uint32:
		return time.Unix(int64(t), 0).Format("02 15:04:05")
	default:
		return fmt.Sprintf("unexpected type %T\n", ts)
	}
}
