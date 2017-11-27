package expr

// aggregation functions for series of data
import (
	"math"

	"github.com/grafana/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type crossSeriesAggFunc func(in []models.Series, out *[]schema.Point)

func getCrossSeriesAggFunc(c string) crossSeriesAggFunc {
	switch c {
	case "avg", "average":
		return crossSeriesAvg
	case "cnt":
		return crossSeriesCnt
	case "lst", "last":
		return crossSeriesLst
	case "min":
		return crossSeriesMin
	case "max":
		return crossSeriesMax
	case "sum":
		return crossSeriesSum
	}
	return nil
}

func crossSeriesAvg(in []models.Series, out *[]schema.Point) {
	for i := 0; i < len(in[0].Datapoints); i++ {
		num := 0
		sum := float64(0)
		for j := 0; j < len(in); j++ {
			p := in[j].Datapoints[i].Val
			if !math.IsNaN(p) {
				num++
				sum += p
			}
		}
		point := schema.Point{
			Ts: in[0].Datapoints[i].Ts,
		}
		if num == 0 {
			point.Val = math.NaN()
		} else {
			point.Val = sum / float64(num)
		}

		*out = append(*out, point)
	}
}

func crossSeriesCnt(in []models.Series, out *[]schema.Point) {
	for i := 0; i < len(in[0].Datapoints); i++ {
		num := 0
		for j := 0; j < len(in); j++ {
			p := in[j].Datapoints[i].Val
			if !math.IsNaN(p) {
				num++
			}
		}
		point := schema.Point{
			Ts: in[0].Datapoints[i].Ts,
		}
		if num == 0 {
			point.Val = math.NaN()
		} else {
			point.Val = float64(num)
		}

		*out = append(*out, point)
	}
}

func crossSeriesLst(in []models.Series, out *[]schema.Point) {
	for i := 0; i < len(in[0].Datapoints); i++ {
		num := math.NaN()
		for j := 0; j < len(in); j++ {
			p := in[j].Datapoints[i].Val
			if !math.IsNaN(p) {
				num = p
			}
		}
		point := schema.Point{
			Ts:  in[0].Datapoints[i].Ts,
			Val: num,
		}

		*out = append(*out, point)
	}
}

func crossSeriesMin(in []models.Series, out *[]schema.Point) {
	for i := 0; i < len(in[0].Datapoints); i++ {
		nan := true
		min := math.NaN()
		for j := 0; j < len(in); j++ {
			p := in[j].Datapoints[i].Val
			if !math.IsNaN(p) {
				nan = false
				min = math.Min(min, p)
			}
		}

		point := schema.Point{
			Ts: in[0].Datapoints[i].Ts,
		}
		if nan {
			point.Val = math.NaN()
		} else {
			point.Val = min
		}

		*out = append(*out, point)
	}
}
func crossSeriesMax(in []models.Series, out *[]schema.Point) {
	for i := 0; i < len(in[0].Datapoints); i++ {
		nan := true
		max := math.Inf(-1)
		for j := 0; j < len(in); j++ {
			p := in[j].Datapoints[i].Val
			if !math.IsNaN(p) {
				nan = false
				max = math.Max(max, p)
			}
		}

		point := schema.Point{
			Ts: in[0].Datapoints[i].Ts,
		}
		if nan {
			point.Val = math.NaN()
		} else {
			point.Val = max
		}

		*out = append(*out, point)
	}
}

func crossSeriesSum(in []models.Series, out *[]schema.Point) {
	for i := 0; i < len(in[0].Datapoints); i++ {
		nan := true
		sum := float64(0)
		for j := 0; j < len(in); j++ {
			p := in[j].Datapoints[i].Val
			if !math.IsNaN(p) {
				nan = false
				sum += p
			}
		}
		point := schema.Point{
			Ts: in[0].Datapoints[i].Ts,
		}
		if nan {
			point.Val = math.NaN()
		} else {
			point.Val = sum
		}

		*out = append(*out, point)
	}
}
