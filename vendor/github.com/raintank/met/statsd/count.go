package statsd

import "github.com/raintank/met"

type Count struct {
	key     string
	backend Backend
}

func (b Backend) NewCount(key string) met.Count {
	c := Count{key, b}
	c.Inc(0)
	return c
}

func (c Count) Inc(val int64) {
	c.backend.client.Count(c.key, int(val), 1)
}
