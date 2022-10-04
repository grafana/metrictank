package stats

import (
	"time"

	"github.com/grafana/metrictank/pkg/clock"
)

func NewDevnull() {
	go func() {
		ticker := clock.AlignedTickLossy(time.Second)
		buf := make([]byte, 0)
		for now := range ticker {
			for _, metric := range registry.list() {
				metric.WriteGraphiteLine(buf[:], nil, now)
			}
		}
	}()
}
