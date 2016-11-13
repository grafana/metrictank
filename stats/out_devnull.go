package stats

import (
	"time"
)

func NewDevnull() {
	go func() {
		ticker := tick(time.Second)
		buf := make([]byte, 0)
		for now := range ticker {
			nowUnix := now.Unix()
			for _, metric := range registry.list() {
				metric.ReportGraphite(nil, buf[:], nowUnix)
			}
		}
	}()
}
