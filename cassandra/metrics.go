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
		cassErrTimeout:                  stats.NewCounter32(fmt.Sprintf("%s.error.timeout", component)),
		cassErrTooManyTimeouts:          stats.NewCounter32(fmt.Sprintf("%s.error.too-many-timeouts", component)),
		cassErrConnClosed:               stats.NewCounter32(fmt.Sprintf("%s.error.conn-closed", component)),
		cassErrNoConns:                  stats.NewCounter32(fmt.Sprintf("%s.error.no-connections", component)),
		cassErrUnavailable:              stats.NewCounter32(fmt.Sprintf("%s.error.unavailable", component)),
		cassErrCannotAchieveConsistency: stats.NewCounter32(fmt.Sprintf("%s.error.cannot-achieve-consistency", component)),
		cassErrOther:                    stats.NewCounter32(fmt.Sprintf("%s.error.other", component)),
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
