package expr

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/grafana/metrictank/api/models"
)

func shuffleStrings(slice []string) []string {
	ret := make([]string, len(slice))
	copy(ret, slice)

	for i := len(ret) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		ret[i], ret[j] = ret[j], ret[i]
	}

	return ret
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
	shuffled := shuffleStrings(lexSort)

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

func BenchmarkSortByName_lexicographical_10000(b *testing.B) {
	benchmarkSortByName(b, false, false, 10000)
}

func BenchmarkSortByName_lexicographical_reverse_10000(b *testing.B) {
	benchmarkSortByName(b, false, true, 10000)
}

func BenchmarkSortByName_natural_10000(b *testing.B) {
	benchmarkSortByName(b, true, false, 10000)
}

func BenchmarkSortByName_natural_reverse_10000(b *testing.B) {
	benchmarkSortByName(b, true, true, 10000)
}

func benchmarkSortByName(b *testing.B, natural, reverse bool, numSeries int) {
	lexSort, natSort := genRandStrings(numSeries)

	shuffled := shuffleStrings(lexSort)
	var in []models.Series
	for _, name := range shuffled {
		in = append(in, models.Series{
			Target: name,
		})
	}

	expected := lexSort
	if natural {
		expected = natSort
	}

	if reverse {
		expected = reverseCopy(expected)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := NewSortByName()
		sort := f.(*FuncSortByName)
		sort.reverse = reverse
		sort.natural = natural
		sort.in = NewMock(in)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("(nat=%t, rev=%t): err should be nil. got %q", natural, reverse, err)
		}
		if len(got) != len(expected) {
			b.Fatalf("(nat=%t, rev=%t): expected %d output series, got %d", natural, reverse, len(expected), len(got))
		}
		for j, o := range expected {
			g := got[j]
			if o != g.Target {
				b.Fatalf("(nat=%t, rev=%t): Mismatch at pos %d. Expected target %q, got %q", natural, reverse, j, o, g.Target)
			}
		}
	}
}
