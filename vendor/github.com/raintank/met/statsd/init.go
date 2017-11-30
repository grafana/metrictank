package statsd

import "gopkg.in/alexcesaro/statsd.v1"

type Backend struct {
	client *statsd.Client
}

// note: library does not auto add ending dot to prefix.
func New(enabled bool, addr, prefix string) (Backend, error) {
	client, err := statsd.New(addr, statsd.WithPrefix(prefix), statsd.Mute(!enabled))
	b := Backend{client}
	return b, err
}
