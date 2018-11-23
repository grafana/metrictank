package main

import (
	"fmt"
	"sort"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

type bucket struct {
	key string
	ttl int
}

type bucketWithCount struct {
	key string
	ttl int
	c   int
}

type byTTL []bucketWithCount

func (a byTTL) Len() int           { return len(a) }
func (a byTTL) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byTTL) Less(i, j int) bool { return a[i].ttl < a[j].ttl }

// shows an overview of all keys and their ttls and closes the iter
// iter must return rows of key and ttl.
func showKeyTTL(iter *gocql.Iter, groupTTL string) {
	roundTTL := 1
	switch groupTTL {
	case "m":
		roundTTL = 60
	case "h":
		roundTTL = 60 * 60
	case "d":
		roundTTL = 60 * 60 * 24
	}

	var b bucket
	bucketMap := make(map[bucket]int)
	for iter.Scan(&b.key, &b.ttl) {
		b.ttl /= roundTTL
		bucketMap[b] += 1
	}

	var bucketList []bucketWithCount
	for b, count := range bucketMap {
		bucketList = append(bucketList, bucketWithCount{
			b.key,
			b.ttl,
			count,
		})
	}

	sort.Sort(byTTL(bucketList))
	for _, b := range bucketList {
		fmt.Printf("%s %d%s %d\n", b.key, b.ttl, groupTTL, b.c)
	}
	err := iter.Close()
	if err != nil {
		log.Errorf("cassandra query error. %s", err)
	}
}
