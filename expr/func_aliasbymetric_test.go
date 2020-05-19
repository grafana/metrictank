package expr

import (
	"testing"

	"github.com/grafana/metrictank/api/models"
)

func TestAliasByMetricZero(t *testing.T) {
	testAlias("zero", []models.Series{}, []models.Series{}, t)
}

func TestAliasByMetricNameWithoutPeriods(t *testing.T) {
	// Metric base same as the metric name
	veryShortMetric := "veryShort"
	veryShortBase := veryShortMetric

	testAliasByMetric(
		[]models.Series{
			// No Function wrapper
			getSeriesNamed(veryShortMetric, a),
			// Function wrapper - single
			getSeries("functionBlah("+veryShortMetric+", funcValue1, funcValue2)", veryShortMetric, a),
			// Function wrapper - multiple
			getSeries("functionBlah(functionBlahBlah("+veryShortMetric+"),funcValue1, funcValue2)", veryShortMetric, a),
		},
		[]models.Series{
			getSeriesNamed(veryShortBase, a),
			getSeriesNamed(veryShortBase, a),
			getSeriesNamed(veryShortBase, a),
		},
		t,
	)
}

func TestAliasByMetricWithoutTags(t *testing.T) {
	// Metric base equals string after the last period
	shortMetric := "my.test.metric.short"
	shortBase := "short"

	testAliasByMetric(
		[]models.Series{
			// No Function wrapper
			getSeriesNamed(shortMetric, a),
			// Function wrapper - single
			getSeries("functionBlah("+shortMetric+", funcValue1, funcValue2)", shortMetric, a),
			// Function wrapper - multiple
			getSeries("functionBlah(functionBlahBlah("+shortMetric+"),funcValue1, funcValue2)", shortMetric, a),
		},
		[]models.Series{
			getSeriesNamed(shortBase, a),
			getSeriesNamed(shortBase, a),
			getSeriesNamed(shortBase, a),
		},
		t,
	)
}

// Long metric string with multiple tag values
// which can accept chars like [a-zA-Z0-9-_./%@ +<>!]
func TestAliasByMetricWithTags(t *testing.T) {
	// Metric base same as the metric name plus
	// the semicolon delimited list of tags
	longMetric := "my.test.metric.long;cluster=abc*;datacenter=some@wher8<>far;version=1.2-3_4.%5;stage=toInfinity;subStage=andBeyond;timezone=OST"
	longBase := "long;cluster=abc*;datacenter=some@wher8<>far;version=1.2-3_4.%5;stage=toInfinity;subStage=andBeyond;timezone=OST"

	testAliasByMetric(
		[]models.Series{
			// No Function wrapper
			getSeriesNamed(longMetric, a),
			// Function wrapper - single
			getSeries("functionBlah("+longMetric+", funcValue1, funcValue2)", longMetric, a),
			// Function wrapper - multiple
			getSeries("functionBlah(functionBlahBlah("+longMetric+"),funcValue1, funcValue2)", longMetric, a),
		},
		[]models.Series{
			getSeriesNamed(longBase, a),
			getSeriesNamed(longBase, a),
			getSeriesNamed(longBase, a),
		},
		t,
	)
}

func testAliasByMetric(in []models.Series, out []models.Series, t *testing.T) {
	f := NewAliasByMetric()
	f.(*FuncAliasByMetric).in = NewMock(in)

	// Copy input to check that it is unchanged later
	inputCopy := make([]models.Series, len(in))
	copy(inputCopy, in)

	dataMap := initDataMap(in)

	got, err := f.Exec(dataMap)
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}

	t.Run("DidNotModifyInput", func(t *testing.T) {
		if err := equalOutput(inputCopy, in, nil, nil); err != nil {
			t.Fatalf("Input was modified, err = %s", err)
		}

	})

	t.Run("DoesNotDoubleReturnPoints", func(t *testing.T) {
		if err := dataMap.CheckForOverlappingPoints(); err != nil {
			t.Fatalf("Point slices in datamap overlap, err = %s", err)
		}
	})
}
