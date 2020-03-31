package expr

import (
	"testing"

	"github.com/grafana/metrictank/api/models"
)

func TestAliasByMetricWithoutTags(t *testing.T) {
	var shortMetric string = "my.test.metric.short"

	testAliasByMetric(
		[]models.Series{
			{ // No Function wrapper
				Interval:   10,
				QueryPatt:  shortMetric,
				Target:     shortMetric,
				Datapoints: getCopy(a),
			},
			{ // Function wrapper - single
				Interval:   10,
				QueryPatt:  shortMetric,
				Target:     "functionBlah(" + shortMetric + ", funcValue1, funcValue2)",
				Datapoints: getCopy(a),
			},
			{ // Function wrapper - multiple
				Interval:   10,
				QueryPatt:  "a",
				Target:     "functionBlah(functionBlahBlah(" + shortMetric + "),funcValue1, funcValue2)",
				Datapoints: getCopy(a),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  shortMetric,
				Target:     shortMetric,
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  shortMetric,
				Target:     shortMetric,
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  shortMetric,
				Target:     shortMetric,
				Datapoints: getCopy(a),
			},
		},
		t,
	)
}

func TestAliasByMetricWithTags(t *testing.T) {
	/* Long metric string with multiple tag values
	   which can accept chars like [a-zA-Z0-9-_./%@ +<>!]
	*/
	var longMetric string = "my.test.metric.long;cluster=abc*;datacenter=some@wher8<>far;version=1.2-3_4.%5;stage=toInfinity;subStage=andBeyond;timezone=OST"

	testAliasByMetric(
		[]models.Series{
			{ // No Function wrapper
				Interval:   10,
				QueryPatt:  longMetric,
				Target:     longMetric,
				Datapoints: getCopy(a),
			},
			{ // Function wrapper - single
				Interval:   10,
				QueryPatt:  longMetric,
				Target:     "functionBlah(" + longMetric + ", funcValue1, funcValue2)",
				Datapoints: getCopy(a),
			},
			{ // Function wrapper - multiple
				Interval:   10,
				QueryPatt:  "a",
				Target:     "functionBlah(functionBlahBlah(" + longMetric + "),funcValue1, funcValue2)",
				Datapoints: getCopy(a),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  longMetric,
				Target:     longMetric,
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  longMetric,
				Target:     longMetric,
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  longMetric,
				Target:     longMetric,
				Datapoints: getCopy(a),
			},
		},
		t,
	)
}

func testAliasByMetric(in []models.Series, out []models.Series, t *testing.T) {
	f := NewAliasByMetric()
	f.(*FuncAliasByMetric).in = NewMock(in)

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}
