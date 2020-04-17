package expr

import (
	"fmt"
	"time"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/raintank/dur"
)

type FuncTimeShift struct {
	in        GraphiteFunc
	timeShift string
	resetEnd  bool
	alignDST  bool

	shiftOffset int
}

func NewTimeShift() GraphiteFunc {
	// TODO - `resetEnd` doesn't seem to work in MT deployments (even when proxying through graphite).
	// I think it is because MT always pads nulls until the end of the requested time range, so we lose
	// when the series actually stops (vs nulls coming from a function like removeBelowValue). Because
	// it doesn't work with MT as a backend, leaving support for it here seems acceptable.
	return &FuncTimeShift{resetEnd: true, alignDST: false}
}

func (s *FuncTimeShift) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgString{val: &s.timeShift, validator: []Validator{IsSignedIntervalString}},
		ArgBool{key: "resetEnd", opt: true, val: &s.resetEnd},
		ArgBool{key: "alignDST", opt: true, val: &s.alignDST},
	}, []Arg{ArgSeriesList{}}
}

func (s *FuncTimeShift) Context(context Context) Context {
	var err error
	s.shiftOffset, err = s.getTimeShiftSeconds(context)
	if err != nil {
		// panic??? TODO - shouldn't happen, validated above
		return context
	}

	// TODO - worry about underflow? uint32 can't represent date older than epoch
	context.from = addOffset(context.from, s.shiftOffset)
	context.to = addOffset(context.to, s.shiftOffset)

	return context
}

func (s *FuncTimeShift) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	negativeOffset := -s.shiftOffset

	formatStr := fmt.Sprintf("timeShift(%%s, \"%s\")", s.timeShift)
	newName := func(oldName string) string {
		return fmt.Sprintf(formatStr, oldName)
	}

	outputs := make([]models.Series, 0, len(series))
	for _, serie := range series {
		out := GetPooledSliceAtLeastSize(len(serie.Datapoints))
		for _, v := range serie.Datapoints {
			out = append(out, schema.Point{Val: v.Val, Ts: addOffset(v.Ts, negativeOffset)})
		}

		output := models.Series{
			Target:       newName(serie.Target),
			QueryPatt:    newName(serie.QueryPatt),
			QueryFrom:    serie.QueryFrom,
			QueryTo:      serie.QueryTo,
			QueryMDP:     serie.QueryMDP,
			QueryPNGroup: serie.QueryPNGroup,
			Tags:         serie.CopyTagsWith("timeShift", s.timeShift),
			Datapoints:   out,
			Interval:     serie.Interval,
			Meta:         serie.Meta,
		}

		outputs = append(outputs, output)
	}
	dataMap.Add(Req{}, outputs...)
	return outputs, nil
}

func (s *FuncTimeShift) getTimeShiftSeconds(context Context) (int, error) {
	// Trim off the sign (if there is one)
	sign := -1
	durStr := s.timeShift
	if durStr[0] == '-' {
		durStr = durStr[1:]
	} else if durStr[0] == '+' {
		sign = 1
		durStr = durStr[1:]
	} else {
		// Normalize timeShift to always include the sign
		s.timeShift = "-" + s.timeShift
	}

	interval, err := dur.ParseDuration(durStr)

	if err != nil {
		return 0, err
	}

	signedOffset := int(interval) * sign

	if s.alignDST {
		timezoneOffset := func(epochTime uint32) int {
			// Use server timezone to determine DST
			localTime := time.Now()
			thenTime := time.Unix(int64(epochTime), 0)

			_, offset := time.Date(thenTime.Year(), thenTime.Month(),
				thenTime.Day(), thenTime.Hour(), thenTime.Minute(),
				thenTime.Second(), thenTime.Nanosecond(), localTime.Location()).Zone()

			return offset
		}

		reqStartOffset := timezoneOffset(context.from)
		reqEndOffset := timezoneOffset(context.to)
		myStartOffset := timezoneOffset(addOffset(context.from, signedOffset))
		myEndOffset := timezoneOffset(addOffset(context.to, signedOffset))

		dstOffset := 0

		// Both requests ranges are entirely in the one offset (either in or out of DST)
		if reqStartOffset == reqEndOffset && myStartOffset == myEndOffset {
			dstOffset = reqStartOffset - myStartOffset
		}

		signedOffset += dstOffset
	}

	return signedOffset, nil
}

func addOffset(orig uint32, offset int) uint32 {
	if offset < 0 {
		orig -= uint32(offset * -1)
	} else {
		orig += uint32(offset)
	}

	return orig
}
