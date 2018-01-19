package expr

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/grafana/metrictank/api/models"
)

func shuffleStrings(slice []string) {
	for i := len(slice) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}
}

func reverseCopy(ss []string) []string {
	last := len(ss) - 1
	copy := make([]string, len(ss))
	for i := 0; i < len(ss); i++ {
		copy[last-i] = ss[i]
	}
	return copy
}

func genRandStrings(numStrings int) ([]string, []string) {
	makeString := func(a, b int) string {
		return fmt.Sprintf("metrictank.stats.env.instance%d.input.plugin%d.metrics_received.counter32", a, b)
	}

	naturalOrdered := make([]string, 0, numStrings)
	for i := 0; i < numStrings; i++ {
		naturalOrdered = append(naturalOrdered, makeString(i/10, i))
	}

	lexicalOrdered := make([]string, numStrings)
	copy(lexicalOrdered, naturalOrdered)
	sort.Strings(lexicalOrdered)

	return lexicalOrdered, naturalOrdered
}

func TestSortByName(t *testing.T) {
	lexSort, natSort := genRandStrings(10000)
	shuffled := make([]string, len(lexSort))
	copy(shuffled, lexSort)
	shuffleStrings(shuffled)

	cases := []struct {
		in            []string
		sorted        []string
		naturalSorted []string
	}{
		{
			[]string{"series.name3.this20", "series.name2.this6", "series.name3.this4"},
			[]string{"series.name2.this6", "series.name3.this20", "series.name3.this4"},
			[]string{"series.name2.this6", "series.name3.this4", "series.name3.this20"},
		},
		{
			[]string{"series00030", "series0003d", "series0003"},
			[]string{"series0003", "series00030", "series0003d"},
			[]string{"series0003", "series0003d", "series00030"},
		},
		{
			shuffled,
			lexSort,
			natSort,
		},
	}
	for i, c := range cases {
		var in []models.Series
		for _, name := range c.in {
			in = append(in, models.Series{
				Target: name,
			})
		}

		expectedResults := []struct {
			natural  bool
			reverse  bool
			expected []string
		}{
			{false, false, c.sorted},
			{false, true, reverseCopy(c.sorted)},
			{true, false, c.naturalSorted},
			{true, true, reverseCopy(c.naturalSorted)},
		}

		for _, e := range expectedResults {
			f := NewSortByName()
			sort := f.(*FuncSortByName)
			sort.reverse = e.reverse
			sort.natural = e.natural
			sort.in = NewMock(in)

			got, err := f.Exec(make(map[Req][]models.Series))
			if err != nil {
				t.Fatalf("case %d (nat=%t, rev=%t): err should be nil. got %q", i, e.natural, e.reverse, err)
			}
			if len(got) != len(e.expected) {
				t.Fatalf("case %d (nat=%t, rev=%t): expected %d output series, got %d", i, e.natural, e.reverse, len(e.expected), len(got))
			}
			for j, o := range e.expected {
				g := got[j]
				if o != g.Target {
					t.Fatalf("case %d (nat=%t, rev=%t): Mismatch at pos %d. Expected target %q, got %q", i, e.natural, e.reverse, j, o, g.Target)
				}
			}
		}
	}
}

/*
func benchmarkSortByName(b *testing.B, natural, reverse, numSeries int) {
	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			Target: fmt.Sprintf("metrictank.stats.env.instance.input.plugin%d.metrics_received.counter32", i),
		}
		input = append(input, series)
	}
	shuffleSeries(input)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sort.Sort(seriesSlice{input, NaturalLess})
	}
}
*/
