package util

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func shuffleSeries(slice []string) {
	for i := len(slice) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}
}

func genRandString(i int) string {
	return fmt.Sprintf("metrictank.stats.env.instance%d.input.plugin%d.metrics_received.counter32", i/10, i)
}

func TestNaturalSort(t *testing.T) {

	numRandStrings := 10000
	randInput := make([]string, 0, numRandStrings)
	for i := 0; i < numRandStrings; i++ {
		randInput = append(randInput, genRandString(i))
	}

	shuffledRandInput := make([]string, numRandStrings)
	copy(shuffledRandInput, randInput)
	shuffleSeries(shuffledRandInput)

	cases := []struct {
		in     []string
		sorted []string
	}{
		{
			[]string{"series.name3.this4", "series.name2.this6", "series.name3.this20"},
			[]string{"series.name2.this6", "series.name3.this4", "series.name3.this20"},
		},
		{
			[]string{"series00030", "series0003d", "series0003"},
			[]string{"series0003", "series0003d", "series00030"},
		},
		{
			shuffledRandInput,
			randInput,
		},
	}
	for i, c := range cases {
		sorted := make([]string, len(c.in))
		copy(sorted, c.in)
		sort.Sort(NaturalSortStringSlice(sorted))

		for j := range sorted {
			if sorted[j] != c.sorted[j] {
				t.Fatalf("case %d: mismatch at pos %d. Expected %s but got %s", i, j, sorted[j], c.sorted[j])
			}
		}
	}
}

func BenchmarkNaturalSort_100(b *testing.B) {
	benchmarkNaturalSort(b, 100)
}

func BenchmarkNaturalSort_10000(b *testing.B) {
	benchmarkNaturalSort(b, 10000)
}

func BenchmarkNaturalSort_100000(b *testing.B) {
	benchmarkNaturalSort(b, 100000)
}

func benchmarkNaturalSort(b *testing.B, numSeries int) {
	var input []string
	for i := 0; i < numSeries; i++ {
		input = append(input, genRandString(i))
	}
	shuffleSeries(input)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sort.Sort(NaturalSortStringSlice(input))
	}
}
