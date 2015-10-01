package main

import "time"
import "fmt"

func ts(t interface{}) string {
	switch a := t.(type) {
	case int64:
		return time.Unix(a, 0).Format("15:04:05")
	case uint32:
		return time.Unix(int64(a), 0).Format("15:04:05")
	default:
		return fmt.Sprintf("unexpected type %T\n", t)
	}
}
