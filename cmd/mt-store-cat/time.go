package main

import (
	"fmt"
	"time"
)

func printTimeUnix(ts uint32) string {
	return fmt.Sprintf("%d", ts)
}

func printTimeFormatted(ts uint32) string {
	return time.Unix(int64(ts), 0).Format(tsFormat)
}
