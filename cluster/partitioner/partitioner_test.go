package partitioner

import (
	"fmt"
	"math"
	"testing"

	"github.com/Shopify/sarama"
)

func TestJumpPartioner3kRtm(t *testing.T) {
	testJumpPartitioner(dataset("rtm"), t)
}
func TestJumpPartioner700kId(t *testing.T) {
	testJumpPartitioner(dataset("id"), t)
}
func TestJumpPartioner1MOps(t *testing.T) {
	testJumpPartitioner(dataset("ops"), t)
}

// note: seems like if n/num_shards is constant, so is score
func TestJumpPartitionerFakeMetrics1kNoTags(t *testing.T) {
	testJumpPartitioner(fakeMetrics(1000, false), t)
}
func TestJumpPartitionerFakeMetrics1kWithTags(t *testing.T) {
	testJumpPartitioner(fakeMetrics(1000, true), t)
}
func TestJumpPartitionerFakeMetrics10kNoTags(t *testing.T) {
	testJumpPartitioner(fakeMetrics(10000, false), t)
}
func TestJumpPartitionerFakeMetrics10kWithTags(t *testing.T) {
	testJumpPartitioner(fakeMetrics(10000, true), t)
}
func TestJumpPartitionerFakeMetrics100kNoTags(t *testing.T) {
	testJumpPartitioner(fakeMetrics(100000, false), t)
}
func TestJumpPartitionerFakeMetrics100kWithTags(t *testing.T) {
	testJumpPartitioner(fakeMetrics(100000, true), t)
}
func TestJumpPartitionerFakeMetrics1MNoTags(t *testing.T) {
	testJumpPartitioner(fakeMetrics(1000000, false), t)
}
func TestJumpPartitionerFakeMetrics1MWithTags(t *testing.T) {
	testJumpPartitioner(fakeMetrics(1000000, true), t)
}
func TestJumpPartitionerFakeMetrics10MNoTags(t *testing.T) {
	testJumpPartitioner(fakeMetrics(10000000, false), t)
}
func TestJumpPartitionerFakeMetrics10MWithTags(t *testing.T) {
	testJumpPartitioner(fakeMetrics(10000000, true), t)
}

func testJumpPartitioner(metrics chan []byte, t *testing.T) {
	jmp := jumpPartitioner{}
	shards := 128
	distributions := make([]int, shards)
	for metric := range metrics {
		msg := &sarama.ProducerMessage{Key: sarama.ByteEncoder(metric)}
		p, err := jmp.Partition(msg, int32(shards))
		if err != nil {
			t.Fatal(err)
		}
		distributions[int(p)]++
	}
	score(distributions, t)
}

// computes the score of a distribution, for which we use the
// coefficient of variation, which is an excellent measure of relative variation.
func score(distributions []int, t *testing.T) {
	var sum int
	for i := 0; i < len(distributions); i++ {
		//		fmt.Printf("%6d", distributions[i])
		if i > 0 && i%30 == 29 {
			//			fmt.Println()
		}
		sum += distributions[i]
	}
	//fmt.Println()
	mean := float64(sum) / float64(len(distributions))

	var stdev float64
	for i := 0; i < len(distributions); i++ {
		stdev += math.Pow(float64(distributions[i])-mean, 2)
	}
	stdev = math.Sqrt(stdev / float64(len(distributions)))
	cov := stdev / mean

	fmt.Println(t.Name(), "score (cov)", cov)
}

var p int32

// note: jumphash is so efficient that trying to test more different metrics would cause slowdowns due to cpu cache misses
// or in the datasets' case, certainly due to file overhead. even trying to manage it via StopTimer and StartTimer is a dead end
// because the overhead of those calls themselves slows stuff down.
func BenchmarkJumpPartitioner(b *testing.B) {
	jmp := jumpPartitioner{}
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
		p, err = jmp.Partition(msgs[i%100], int32(i%512))
		if err != nil {
			b.Fatal(err)
		}
	}
}
