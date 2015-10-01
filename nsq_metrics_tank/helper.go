package main

import "time"

func ts(t uint32) string {
	return time.Unix(int64(t), 0).Format("15:04:05")
}
