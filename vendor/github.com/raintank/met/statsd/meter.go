// it's commonly used for non-timer cases where we want these summaries, that's
// what this is for.
package statsd

import "github.com/raintank/met"

type Meter struct {
	key     string
	backend Backend
}

func (b Backend) NewMeter(key string, val int64) met.Meter {
	m := Meter{key, b}
	m.Value(val)
	return m
}

func (m Meter) Value(val int64) {
	m.backend.client.Timing(m.key, int(val), 1)
}
