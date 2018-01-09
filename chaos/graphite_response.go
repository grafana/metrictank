package chaos

import (
	"bytes"
	"errors"
	"math"
	"strconv"
)

type Response []Series

type Series struct {
	Target     string
	Datapoints []Point
}

type Point struct {
	Val float64
	Ts  uint32
}

var errInvalidFormat = errors.New("invalid format")

func (p *Point) UnmarshalJSON(data []byte) error {
	if len(data) < 2 {
		return errInvalidFormat
	}
	// find first digit or 'n' for "null"
	for (data[0] < 48 || data[0] > 57) && data[0] != 110 {
		if len(data) == 1 {
			return errInvalidFormat
		}
		data = data[1:]
	}
	// find comma
	var i int
	for i = 0; i < len(data); i++ {
		if data[i] == 44 {
			break
		}
	}
	if i == 0 {
		return errInvalidFormat
	}

	if bytes.HasPrefix(data[:i], []byte("null")) {
		p.Val = math.NaN()
	} else {
		fl, err := strconv.ParseFloat(string(data[:i]), 64)
		if err != nil {
			return err
		}
		p.Val = fl
	}
	data = data[i:]
	if len(data) < 2 {
		return errInvalidFormat
	}

	// find first digit
	for (data[0] < 48 || data[0] > 57) && data[0] != 110 {
		if len(data) == 1 {
			return errInvalidFormat
		}
		data = data[1:]
	}
	// find last digit
	for i = 0; data[i] >= 48 && data[i] <= 57 && i < len(data); i++ {
	}
	if i == 0 {
		return errInvalidFormat
	}

	ts, err := strconv.ParseUint(string(data[:i]), 10, 32)
	if err != nil {
		return err
	}
	p.Ts = uint32(ts)
	return nil
}
