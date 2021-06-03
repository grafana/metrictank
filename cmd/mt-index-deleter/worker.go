package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigtable"
	log "github.com/sirupsen/logrus"
)

var deleted uint64
var workerStart time.Time

func workerStats() {
	dur := time.Since(workerStart)
	del := atomic.LoadUint64(&deleted)
	deletesPerSec := float64(del) / dur.Seconds()
	log.Infof("workerstats: %d deleted in %v (%f deletes/s)", del, dur, deletesPerSec)

}

// deletes all keys seen on the channel, in batches of size batchSize
// if an error occurs, it's printed and the function returns
func worker(wg *sync.WaitGroup, c btClient, batchSize int, deletes chan string) {
	defer wg.Done()
	muts := make([]*bigtable.Mutation, 0, batchSize)
	keys := make([]string, 0, batchSize)

	flush := func() error {
		if len(muts) == 0 {
			return nil
		}
		errs, err := c.tbl.ApplyBulk(context.Background(), keys, muts)
		l := len(muts)
		muts = muts[:0]
		keys = keys[:0]
		if err != nil {
			return err
		}
		if len(errs) > 0 {
			return fmt.Errorf("saw %d errors. first one: %v", len(errs), errs[0])
		}

		atomic.AddUint64(&deleted, uint64(l))

		return nil
	}

	for {
		key, ok := <-deletes
		if !ok {
			log.Info("worker seeing deletes close. flushing last batch")
			err := flush()
			if err != nil {
				log.Errorf("worker error: %v", err)
			}
			return
		}
		mut := bigtable.NewMutation()
		mut.DeleteRow()
		muts = append(muts, mut)
		keys = append(keys, key)
		if len(muts) == batchSize {
			err := flush()
			if err != nil {
				log.Errorf("worker error: %v", err)
				return
			}
		}
	}
}
