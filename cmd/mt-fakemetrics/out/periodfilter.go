package out

import (
	"errors"
	"regexp"
	"strconv"
	"strings"

	"github.com/grafana/metrictank/internal/schema"
)

type rule struct {
	patt     *regexp.Regexp
	interval int
}

// PeriodFilter adjusts the reported interval property for outgoing metrics
// (not the actual period at which data is sent!)
// for any metric, the first rule matched wins
type PeriodFilter struct {
	out   Out
	rules []rule
}

// patt=interval:patt2=interval
func NewPeriodFilter(out Out, opts string) (PeriodFilter, error) {

	f := PeriodFilter{
		out: out,
	}

	if len(opts) == 0 {
		return f, errors.New("no options specified")
	}

	ruleStrings := strings.Split(opts, ":")
	for _, ruleString := range ruleStrings {
		split := strings.Split(ruleString, "=")
		if len(split) != 2 {
			return f, errors.New("option must be 'regex=interval'")
		}
		var r rule
		var err error
		r.patt, err = regexp.Compile(split[0])
		if err != nil {
			return f, errors.New("option must be 'regex=interval'")
		}
		r.interval, err = strconv.Atoi(split[1])
		if err != nil {
			return f, errors.New("option must be 'regex=interval'")
		}
		f.rules = append(f.rules, r)
	}

	return f, nil
}

func (pf PeriodFilter) Close() error {
	return pf.out.Close()
}

func (pf PeriodFilter) Flush(metrics []*schema.MetricData) error {
	for i := range metrics {
		for _, rule := range pf.rules {
			if rule.patt.MatchString(metrics[i].Name) {
				metrics[i].Interval = rule.interval
				break
			}
		}
	}
	return pf.out.Flush(metrics)
}
