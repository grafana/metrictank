package persister

/*
Schemas read code from https://github.com/grobian/carbonwriter/
*/

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/alyu/configparser"
	"github.com/lomik/go-whisper"
)

type WhisperAggregationItem struct {
	name                 string
	pattern              *regexp.Regexp
	XFilesFactor         float64
	aggregationMethodStr string
	AggregationMethod    []whisper.AggregationMethod
}

// WhisperAggregation ...
type WhisperAggregation struct {
	Data    []WhisperAggregationItem
	Default WhisperAggregationItem
}

// NewWhisperAggregation create instance of WhisperAggregation
func NewWhisperAggregation() WhisperAggregation {
	return WhisperAggregation{
		Data: make([]WhisperAggregationItem, 0),
		Default: WhisperAggregationItem{
			name:                 "default",
			pattern:              nil,
			XFilesFactor:         0.5,
			aggregationMethodStr: "average",
			AggregationMethod:    []whisper.AggregationMethod{whisper.Average},
		},
	}
}

// ReadWhisperAggregation ...
func ReadWhisperAggregation(file string) (WhisperAggregation, error) {
	config, err := configparser.Read(file)
	if err != nil {
		return WhisperAggregation{}, err
	}
	// pp.Println(config)
	sections, err := config.AllSections()
	if err != nil {
		return WhisperAggregation{}, err
	}

	result := NewWhisperAggregation()

	for _, s := range sections {
		item := WhisperAggregationItem{}
		// this is mildly stupid, but I don't feel like forking
		// configparser just for this
		item.name =
			strings.Trim(strings.SplitN(s.String(), "\n", 2)[0], " []")
		if item.name == "" || strings.HasPrefix(item.name, "#") {
			continue
		}

		item.pattern, err = regexp.Compile(s.ValueOf("pattern"))
		if err != nil {
			logrus.Errorf("[persister] Failed to parse pattern '%s'for [%s]: %s",
				s.ValueOf("pattern"), item.name, err.Error())
			return WhisperAggregation{}, err
		}

		item.XFilesFactor, err = strconv.ParseFloat(s.ValueOf("xFilesFactor"), 64)
		if err != nil {
			logrus.Errorf("failed to parse xFilesFactor '%s' in %s: %s",
				s.ValueOf("xFilesFactor"), item.name, err.Error())
			return WhisperAggregation{}, err
		}

		item.aggregationMethodStr = s.ValueOf("aggregationMethod")
		methodStrs := strings.Split(item.aggregationMethodStr, ",")
		for _, methodStr := range methodStrs {
			switch methodStr {
			case "average", "avg":
				item.AggregationMethod = append(item.AggregationMethod, whisper.Average)
			case "sum":
				item.AggregationMethod = append(item.AggregationMethod, whisper.Sum)
			case "last":
				item.AggregationMethod = append(item.AggregationMethod, whisper.Last)
			case "max":
				item.AggregationMethod = append(item.AggregationMethod, whisper.Max)
			case "min":
				item.AggregationMethod = append(item.AggregationMethod, whisper.Min)
			default:
				logrus.Errorf("unknown aggregation method '%s'", methodStr)
				return result, fmt.Errorf("unknown aggregation method %q", methodStr)
			}
		}

		logrus.Debugf("[persister] Adding aggregation [%s] pattern = %s aggregationMethod = %s xFilesFactor = %f",
			item.name, s.ValueOf("pattern"),
			item.aggregationMethodStr, item.XFilesFactor)

		result.Data = append(result.Data, item)
	}

	return result, nil
}

// Match find schema for metric
func (a *WhisperAggregation) Match(metric string) (uint16, WhisperAggregationItem) {
	for i, s := range a.Data {
		if s.pattern.MatchString(metric) {
			return uint16(i), s
		}
	}
	return uint16(len(a.Data)), a.Default // default has the index of one more than last of what's in a.Data
}
