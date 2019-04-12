package partitioner

import (
	"fmt"
	"math"
	"testing"

	"github.com/Shopify/sarama"
)

// note: lots of output here, to make sense of it, here's some tips:
// * generally, COV should go down as totalMetrics/n increases.
//   in particular, low metric counts on high number of shards has bad scores
//   but is also less relevant.
// * fnv = medium (32 partitions)
// * ops = medium (32 partitions)
// * rtm = small (8 partitions)
// * id = don't remember which instance this is

func TestSaramaPartitioner(t *testing.T) {
	testPartitioner("sarama", sarama.NewHashPartitioner(""), t)
}

func TestJumpPartitionerMauro(t *testing.T) {
	testPartitioner("jump-mauro", jumpPartitionerMauro{}, t)
}
func TestJumpPartitionerFnv(t *testing.T) {
	testPartitioner("jump-fnv", jumpPartitionerFnv{}, t)
}
func TestJumpPartitionerMetro(t *testing.T) {
	testPartitioner("jump-metro", jumpPartitionerMetro{}, t)
}
func TestJumpPartitionerSip(t *testing.T) {
	testPartitioner("jump-sip", jumpPartitionerSip{}, t)
}
func TestJumpPartitionerXxhash(t *testing.T) {
	testPartitioner("jump-xxhash", jumpPartitionerXxhash{}, t)
}

func testPartitioner(partitionerName string, p sarama.Partitioner, t *testing.T) {
	test := func(datasetName string, shards int, dataset chan []byte, t *testing.T) {
		distributions := make([]int, shards)
		var total int
		for metric := range dataset {
			msg := &sarama.ProducerMessage{Key: sarama.ByteEncoder(metric)}
			p, err := p.Partition(msg, int32(shards))
			if err != nil {
				t.Fatal(err)
			}
			distributions[int(p)]++
			total++
		}
		cov, pctDiff := stats(distributions, t)
		fmt.Printf("%20s/%-4d %10s %9d -> cov=%.3f, diff=%.2f%%\n", partitionerName, shards, datasetName, total, cov, pctDiff)
	}

	for _, shards := range []int{8, 32, 128} {
		test("rtm", shards, dataset("rtm"), t)
		test("id", shards, dataset("id"), t)
		test("ops", shards, dataset("ops"), t)
		test("fng", shards, dataset("fng"), t)
		for _, tags := range []bool{false, true} {
			for _, fakes := range []int{10 * 1000, 100 * 1000, 1000 * 1000, 10 * 1000 * 1000} {
				datasetName := fmt.Sprintf("fake-%t", tags)
				test(datasetName, shards, fakeMetrics(fakes, tags), t)
			}
		}
	}
}

// stats computes the stats of the distribution:
// 1) coefficient of variation, which is an excellent measure of relative variation.
// 2) percent difference between highest and lowest partition
func stats(distributions []int, t *testing.T) (float64, float64) {
	var sum int
	min := math.MaxInt64
	max := math.MinInt64
	for i := 0; i < len(distributions); i++ {
		// fmt.Printf("%6d", distributions[i])
		size := distributions[i]
		if size < min {
			min = size
		}
		if size > max {
			max = size
		}
		sum += size
	}
	mean := float64(sum) / float64(len(distributions))

	var stdev float64
	for i := 0; i < len(distributions); i++ {
		stdev += math.Pow(float64(distributions[i])-mean, 2)
	}
	stdev = math.Sqrt(stdev / float64(len(distributions)))
	cov := stdev / mean
	pctdiff := float64(100*(max-min)) / float64(min)
	return cov, pctdiff
}

var p int32

func BenchmarkPartitionerSarama(b *testing.B) {
	benchmarkPartitioner(sarama.NewHashPartitioner(""), b)
}
func BenchmarkJumpPartitionerMauro(b *testing.B) {
	benchmarkPartitioner(jumpPartitionerMauro{}, b)
}
func BenchmarkJumpPartitionerFnv(b *testing.B) {
	benchmarkPartitioner(jumpPartitionerFnv{}, b)
}
func BenchmarkJumpPartitionerMetro(b *testing.B) {
	benchmarkPartitioner(jumpPartitionerMetro{}, b)
}
func BenchmarkJumpPartitionerSip(b *testing.B) {
	benchmarkPartitioner(jumpPartitionerSip{}, b)
}
func BenchmarkJumpPartitionerXxhash(b *testing.B) {
	benchmarkPartitioner(jumpPartitionerXxhash{}, b)
}

// note: jumphash is so efficient that trying to test more different metrics would cause slowdowns due to cpu cache misses
// or in the datasets' case, certainly due to file overhead. even trying to manage it via StopTimer and StartTimer is a dead end
// because the overhead of those calls themselves slows stuff down.
func benchmarkPartitioner(partitioner sarama.Partitioner, b *testing.B) {
	k := []byte("subtree.app_foobar.stats.dc11.some.id.of.aha.usually_some_long_word_here.metric.upper_16.counter128;with_some=tags;id=some;this=seems.rather.realistic") // len=150

	// we want varying length input keys
	// because some slice lengths get an underlying array with spare capacity
	// while others don't get so much
	// so we want to test out the input space: 100 msgs with keys varying from len 51 to 150
	var msgs [100]*sarama.ProducerMessage
	for i := 0; i < 100; i++ {
		key := make([]byte, 50+i+1)
		copy(key, k)
		msgs[i] = &sarama.ProducerMessage{Key: sarama.ByteEncoder(key)}
	}

	var err error

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		p, err = partitioner.Partition(msgs[i%100], 128)
		if err != nil {
			b.Fatal(err)
		}
	}
}
