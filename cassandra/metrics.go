package cassandra

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/raintank/met"
)

type Metrics struct {
	cassErrTimeout         met.Count
	cassErrTooManyTimeouts met.Count
	cassErrConnClosed      met.Count
	cassErrNoConns         met.Count
	cassErrUnavailable     met.Count
	cassErrOther           met.Count
}

func (m *Metrics) Init(component string, stats met.Backend) {
	m.cassErrTimeout = stats.NewCount(fmt.Sprintf("%s.error.timeout", component))
	m.cassErrTooManyTimeouts = stats.NewCount(fmt.Sprintf("%s.error.too-many-timeouts", component))
	m.cassErrConnClosed = stats.NewCount(fmt.Sprintf("%s.error.conn-closed", component))
	m.cassErrNoConns = stats.NewCount(fmt.Sprintf("%s.error.no-connections", component))
	m.cassErrUnavailable = stats.NewCount(fmt.Sprintf("%s.error.unavailable", component))
	m.cassErrOther = stats.NewCount(fmt.Sprintf("%s.error.other", component))
}

func (m *Metrics) Inc(err error) {
	if err == gocql.ErrTimeoutNoResponse {
		m.cassErrTimeout.Inc(1)
	} else if err == gocql.ErrTooManyTimeouts {
		m.cassErrTooManyTimeouts.Inc(1)
	} else if err == gocql.ErrConnectionClosed {
		m.cassErrConnClosed.Inc(1)
	} else if err == gocql.ErrNoConnections {
		m.cassErrNoConns.Inc(1)
	} else if err == gocql.ErrUnavailable {
		m.cassErrUnavailable.Inc(1)
	} else {
		m.cassErrOther.Inc(1)
	}
}
