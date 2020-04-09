package expr

import (
	"testing"

	"github.com/grafana/metrictank/api/models"
)

func TestAliasByMetricNameWithoutPeriods(t *testing.T) {
	veryShortMetric := "veryShort"
	tags, rTags := makeTagsMaps(veryShortMetric, veryShortMetric)

	testAliasByMetric(
		[]models.Series{
			{ // No Function wrapper
				Interval:   10,
				QueryPatt:  veryShortMetric,
				Target:     veryShortMetric,
				Tags:       tags,
				Datapoints: getCopy(a),
			},
			{ // Function wrapper - single
				Interval:   10,
				QueryPatt:  veryShortMetric,
				Target:     "functionBlah(" + veryShortMetric + ", funcValue1, funcValue2)",
				Tags:       tags,
				Datapoints: getCopy(a),
			},
			{ // Function wrapper - multiple
				Interval:   10,
				QueryPatt:  veryShortMetric,
				Target:     "functionBlah(functionBlahBlah(" + veryShortMetric + "),funcValue1, funcValue2)",
				Tags:       tags,
				Datapoints: getCopy(a),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  veryShortMetric,
				Target:     veryShortMetric,
				Tags:       rTags,
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  veryShortMetric,
				Target:     veryShortMetric,
				Tags:       rTags,
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  veryShortMetric,
				Target:     veryShortMetric,
				Tags:       rTags,
				Datapoints: getCopy(a),
			},
		},
		t,
	)
}

func TestAliasByMetricWithoutTags(t *testing.T) {
	shortMetric := "my.test.metric.short"
	tags, rTags := makeTagsMaps(shortMetric, "short")

	testAliasByMetric(
		[]models.Series{
			{ // No Function wrapper
				Interval:   10,
				QueryPatt:  shortMetric,
				Target:     shortMetric,
				Tags:       tags,
				Datapoints: getCopy(a),
			},
			{ // Function wrapper - single
				Interval:   10,
				QueryPatt:  shortMetric,
				Target:     "functionBlah(" + shortMetric + ", funcValue1, funcValue2)",
				Tags:       tags,
				Datapoints: getCopy(a),
			},
			{ // Function wrapper - multiple
				Interval:   10,
				QueryPatt:  "a",
				Target:     "functionBlah(functionBlahBlah(" + shortMetric + "),funcValue1, funcValue2)",
				Tags:       tags,
				Datapoints: getCopy(a),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "short",
				Target:     "short",
				Tags:       rTags,
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "short",
				Target:     "short",
				Tags:       rTags,
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "short",
				Target:     "short",
				Tags:       rTags,
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
	longMetric := "my.test.metric.long;cluster=abc*;datacenter=some@wher8<>far;version=1.2-3_4.%5;stage=toInfinity;subStage=andBeyond;timezone=OST"
	tags, rTags := makeTagsMaps(longMetric, "long")

	testAliasByMetric(
		[]models.Series{
			{ // No Function wrapper
				Interval:   10,
				QueryPatt:  longMetric,
				Target:     longMetric,
				Tags:       tags,
				Datapoints: getCopy(a),
			},
			{ // Function wrapper - single
				Interval:   10,
				QueryPatt:  longMetric,
				Target:     "functionBlah(" + longMetric + ", funcValue1, funcValue2)",
				Tags:       tags,
				Datapoints: getCopy(a),
			},
			{ // Function wrapper - multiple
				Interval:   10,
				QueryPatt:  "a",
				Target:     "functionBlah(functionBlahBlah(" + longMetric + "),funcValue1, funcValue2)",
				Tags:       tags,
				Datapoints: getCopy(a),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "long",
				Target:     "long",
				Tags:       rTags,
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "long",
				Target:     "long",
				Tags:       rTags,
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "long",
				Target:     "long",
				Tags:       rTags,
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

func makeTagsMaps(beforeName string, afterName string) (map[string]string, map[string]string) {
	numTags := 2

	before := make(map[string]string, numTags)
	after := make(map[string]string, numTags)

	for i := 1; i < numTags; i++ {
		strI := string(i)

		before["key_"+strI] = "val_" + strI
		after["key_"+strI] = "val_" + strI
	}

	before["name"] = beforeName
	after["name"] = afterName

	return before, after
}
