package partitioner

import (
	"testing"

	"github.com/Shopify/sarama"
)

var p int32

func BenchmarkJumpPartitioner(b *testing.B) {
	benchmarkPartitioner(jumpPartitioner{}, b)
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
