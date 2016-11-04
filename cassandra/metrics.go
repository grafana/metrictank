package cassandra

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/raintank/met"
)

type Metrics struct {
	cassErrTimeout                  met.Count
	cassErrTooManyTimeouts          met.Count
	cassErrConnClosed               met.Count
	cassErrNoConns                  met.Count
	cassErrUnavailable              met.Count
	cassErrCannotAchieveConsistency met.Count
	cassErrOther                    met.Count
}

func NewMetrics(component string, stats met.Backend) Metrics {
	return Metrics{
		cassErrTimeout:                  stats.NewCount(fmt.Sprintf("%s.error.timeout", component)),
		cassErrTooManyTimeouts:          stats.NewCount(fmt.Sprintf("%s.error.too-many-timeouts", component)),
		cassErrConnClosed:               stats.NewCount(fmt.Sprintf("%s.error.conn-closed", component)),
		cassErrNoConns:                  stats.NewCount(fmt.Sprintf("%s.error.no-connections", component)),
		cassErrUnavailable:              stats.NewCount(fmt.Sprintf("%s.error.unavailable", component)),
		cassErrCannotAchieveConsistency: stats.NewCount(fmt.Sprintf("%s.error.cannot-achieve-consistency", component)),
		cassErrOther:                    stats.NewCount(fmt.Sprintf("%s.error.other", component)),
	}
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
	} else if strings.HasPrefix(err.Error(), "Cannot achieve consistency level") {
		m.cassErrCannotAchieveConsistency.Inc(1)
	} else {
		m.cassErrOther.Inc(1)
	}
}
