package main

import (
	"bytes"
	"fmt"
	"github.com/boltdb/bolt"
	"math/rand"
	"os"
	"sort"
	"testing"
)

var keysRand [][]byte
var keysSort [][]byte
var keysRevSort [][]byte

type sortableKeys [][]byte

func (s sortableKeys) Less(i, j int) bool {
	return string(s[i]) < string(s[j])
}

func (s sortableKeys) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortableKeys) Len() int {
	return len(s)
}

func init() {
	// the keys themselves are random, so in random order, so we can just iterate this slice in sequence
	// to simulate the random writes
	keysRand = make([][]byte, 1000000)
	keysSort = make([][]byte, 1000000)
	keysRevSort = make([][]byte, 1000000)
	for i := 0; i < 1000000; i++ {
		keysRand[i] = randKey(10)
	}
	copy(keysSort, keysRand)
	copy(keysRevSort, keysRand)
	s := sortableKeys(keysSort)
	sort.Sort(s)
	keysSort = [][]byte(s)
	sort.Reverse(s)
	keysRevSort = [][]byte(s)
}

// returns a chunk for testing of 100Bytes
func getChunk(id int) []byte {
	b := bytes.NewBuffer([]byte(fmt.Sprintf("%20d", id)))
	_, err := b.Write([]byte("11111111112222222222333333333344444444445555555555666666666677777777778888888888"))
	if err != nil {
		panic(err)
	}
	return b.Bytes()
}

// see http://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randKey(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Int63()%int64(len(letters))]
	}
	return b
}

func BenchmarkBoltWrite1kRandMetrics(b *testing.B) {
	benchmarkBoltWrite(keysRand[0:1000], b)
}
func BenchmarkBoltWrite100kRandMetrics(b *testing.B) {
	benchmarkBoltWrite(keysRand[0:100000], b)
}
func BenchmarkBoltWrite1kSortMetrics(b *testing.B) {
	benchmarkBoltWrite(keysSort[0:1000], b)
}
func BenchmarkBoltWrite100kSortMetrics(b *testing.B) {
	benchmarkBoltWrite(keysSort[0:100000], b)
}
func BenchmarkBoltWrite1kRevSortMetrics(b *testing.B) {
	benchmarkBoltWrite(keysRevSort[0:1000], b)
}
func BenchmarkBoltWrite100kRevSortMetrics(b *testing.B) {
	benchmarkBoltWrite(keysRevSort[0:100000], b)
}
func benchmarkBoltWrite(keys [][]byte, b *testing.B) {
	fname := fmt.Sprintf("test-%d.db", len(keys))
	db, err := bolt.Open(fname, 0600, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.Update(func(tx *bolt.Tx) error {
		for metric := 0; metric < len(keys); metric++ {
			_, err := tx.CreateBucketIfNotExists(keys[metric])
			if err != nil {
				return fmt.Errorf("create bucket: %s", err)
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	b.StartTimer()
	// N is number of chunks.
	db.Update(func(tx *bolt.Tx) error {
		for chunk := 0; chunk < b.N; chunk++ {
			//fmt.Print(chunk)
			for metric := 0; metric < len(keys); metric++ {
				b := tx.Bucket(keys[metric])
				err := b.Put([]byte(string(chunk)), getChunk(chunk))
				if err != nil {
					return err
				}
				//if metric%(len(keys)/50) == 0 {
				//		fmt.Print(".")
				//	}
			}
			//fmt.Println()
			b.SetBytes(int64(len(keys)) * 100)
		}
		return nil
	})
	b.StopTimer()
	printB := func(bytes int64) string {
		if bytes < 4096 {
			return fmt.Sprintf("%d B", bytes)
		} else if bytes < 4096*1024 {
			return fmt.Sprintf("%d KiB", bytes/1024)
		} else if bytes < 4096*1024*10024 {
			return fmt.Sprintf("%d MiB", bytes/1024/1024)
		} else {
			return fmt.Sprintf("%d GiB", bytes/1024/1024/1024)
		}
	}
	totalB := int64(len(keys) * 100 * b.N)
	st, err := os.Stat(fname)
	if err != nil {
		panic(err)
	}
	b.Logf("wrote %s -- filesize %s", printB(totalB), printB(st.Size()))
	os.Remove(fname)
	/*	for chunk := 0; chunk < b.N; chunk++ {
			for metric := 0; metric < m; metric++ {
				db.View(func(tx *bolt.Tx) error {
					b := tx.Bucket(metricKeys[metric])
					v := b.Get([]byte(string(chunk)))
					fmt.Printf("The answer is: %s\n", v)
					return nil
				})
			}
		}
	*/
}
