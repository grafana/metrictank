package partitioner

import (
	"bufio"
	"fmt"
	"os"
)

// loads a text file with a metricNameWithTag per line
func dataset(name string) chan []byte {
	file, err := os.Open("datasets/" + name + ".txt")
	if err != nil {
		panic(err)
	}

	scanner := bufio.NewScanner(file)
	metrics := make(chan []byte)

	go func(metrics chan []byte) {
		for scanner.Scan() {
			buf := scanner.Bytes()
			bufCopy := make([]byte, len(buf))
			copy(bufCopy, buf)
			metrics <- bufCopy
		}

		if err := scanner.Err(); err != nil {
			panic(err)
		}
		close(metrics)
		file.Close()
	}(metrics)
	return metrics
}

// fakeMetrics generates metric names just like the fakemetrics tool does
func fakeMetrics(num int, addTags bool) chan []byte {
	metrics := make(chan []byte)
	go func(metrics chan []byte) {
		for i := 1; i <= num; i++ {
			if addTags {
				metrics <- []byte(fmt.Sprintf("some.id.of.a.metric.%d;id=%d;some=tag", i, i))
			} else {
				metrics <- []byte(fmt.Sprintf("some.id.of.a.metric.%d", i))
			}
		}
		close(metrics)
	}(metrics)
	return metrics
}
