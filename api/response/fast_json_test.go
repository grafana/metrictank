package response

import (
	"net/http/httptest"
	"testing"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

func TestFastJson(t *testing.T) {
	for _, c := range testSeries() {
		w := httptest.NewRecorder()
		Write(w, NewFastJson(200, models.SeriesByTarget(c.in)))
		got := w.Body.String()
		if c.out != got {
			t.Fatalf("bad json output.\nexpected:%s\ngot:     %s\n", c.out, got)
		}
	}
}

func BenchmarkHttpRespFastJson(b *testing.B) {
	pA := make([]schema.Point, 1000, 1000)
	pB := make([]schema.Point, 1000, 1000)
	baseTs := 1500000000
	for i := 0; i < 1000; i++ {
		pA[i] = schema.Point{Val: float64(10000 * i), Ts: uint32(baseTs + 10*i)}
		pB[i] = schema.Point{Val: 12.34 * float64(i), Ts: uint32(baseTs + 10*i)}
	}
	data := []models.Series{
		{
			Target:     "some.metric.with.a-whole-bunch-of.integers",
			Datapoints: pA,
			Interval:   10,
		},
		{
			Target:     "some.metric.with.a-whole-bunch-of.floats",
			Datapoints: pB,
			Interval:   10,
		},
	}
	b.SetBytes(int64(len(pA) * 2 * 12))

	b.ResetTimer()
	var resp *FastJson
	for n := 0; n < b.N; n++ {
		resp = NewFastJson(200, models.SeriesByTarget(data))
		resp.Body()
		resp.Close()
	}
}
