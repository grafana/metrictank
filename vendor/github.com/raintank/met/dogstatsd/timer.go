package dogstatsd

import "time"
import "github.com/raintank/met"

// note that due the preseeding in init, you shouldn't rely on the count and count_ps summaries
// rather, consider maintaining a separate counter
// see https://github.com/raintank/grafana/issues/133

type Timer struct {
	key     string
	backend Backend
}

func (b Backend) NewTimer(key string, val time.Duration) met.Timer {
	t := Timer{key, b}
	t.Value(val)
	return t
}

func (t Timer) Value(val time.Duration) {
	t.backend.client.TimeInMilliseconds(t.key, val.Seconds()*1000, []string{}, 1)
}
