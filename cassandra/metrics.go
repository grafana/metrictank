package cassandra

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/raintank/metrictank/stats"
)

type ErrMetrics struct {
	cassErrTimeout                  *stats.Counter32
	cassErrTooManyTimeouts          *stats.Counter32
	cassErrConnClosed               *stats.Counter32
	cassErrNoConns                  *stats.Counter32
	cassErrUnavailable              *stats.Counter32
	cassErrCannotAchieveConsistency *stats.Counter32
	cassErrOther                    *stats.Counter32
}

func NewErrMetrics(component string) ErrMetrics {
	return ErrMetrics{

		// metric idx.cassandra.error.timeout is a counter of timeouts seen to the cassandra idx

		// metric store.cassandra.error.timeout is a counter of timeouts seen to the cassandra store
		cassErrTimeout: stats.NewCounter32(fmt.Sprintf("%s.error.timeout", component)),

		// metric idx.cassandra.error.too-many-timeouts is a counter of how many times we saw to many timeouts and closed the connection to the cassandra idx

		// metric store.cassandra.error.too-many-timeouts is a counter of how many times we saw to many timeouts and closed the connection to the cassandra store
		cassErrTooManyTimeouts: stats.NewCounter32(fmt.Sprintf("%s.error.too-many-timeouts", component)),

		// metric idx.cassandra.error.conn-closed is a counter of how many times we saw a connection closed to the cassandra idx

		// metric store.cassandra.error.conn-closed is a counter of how many times we saw a connection closed to the cassandra store
		cassErrConnClosed: stats.NewCounter32(fmt.Sprintf("%s.error.conn-closed", component)),

		// metric idx.cassandra.error.no-connections is a counter of how many times we had no connections remaining to the cassandra idx

		// metric store.cassandra.error.no-connections is a counter of how many times we had no connections remaining to the cassandra store
		cassErrNoConns: stats.NewCounter32(fmt.Sprintf("%s.error.no-connections", component)),

		// metric idx.cassandra.error.unavailable is a counter of how many times the cassandra idx was unavailable

		// metric store.cassandra.error.unavailable is a counter of how many times the cassandra store was unavailable
		cassErrUnavailable: stats.NewCounter32(fmt.Sprintf("%s.error.unavailable", component)),

		// metric idx.cassandra.error.cannot-achieve-consistency is a counter of the cassandra idx not being able to achieve consistency for a given query

		// metric store.cassandra.error.cannot-achieve-consistency is a counter of the cassandra store not being able to achieve consistency for a given query
		cassErrCannotAchieveConsistency: stats.NewCounter32(fmt.Sprintf("%s.error.cannot-achieve-consistency", component)),

		// metric idx.cassandra.error.other is a counter of other errors talking to the cassandra idx

		// metric store.cassandra.error.other is a counter of other errors talking to the cassandra store
		cassErrOther: stats.NewCounter32(fmt.Sprintf("%s.error.other", component)),
	}
}

func (m *ErrMetrics) Inc(err error) {
	if err == gocql.ErrTimeoutNoResponse {
		m.cassErrTimeout.Inc()
	} else if err == gocql.ErrTooManyTimeouts {
		m.cassErrTooManyTimeouts.Inc()
	} else if err == gocql.ErrConnectionClosed {
		m.cassErrConnClosed.Inc()
	} else if err == gocql.ErrNoConnections {
		m.cassErrNoConns.Inc()
	} else if err == gocql.ErrUnavailable {
		m.cassErrUnavailable.Inc()
	} else if strings.HasPrefix(err.Error(), "Cannot achieve consistency level") {
		m.cassErrCannotAchieveConsistency.Inc()
	} else {
		m.cassErrOther.Inc()
	}
}
