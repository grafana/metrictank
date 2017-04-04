package main

import (
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/gocql/gocql"
)

// shows an overview of all keys and their ttls and closes the iter
// iter must return rows of key and ttl.
func showKeyTTL(iter *gocql.Iter, roundTTL int) {
	var key, prevKey string
	var ttl, prevTTL, cnt int
	for iter.Scan(&key, &ttl) {
		ttl = ttl / roundTTL
		if ttl == prevTTL && key == prevKey {
			cnt += 1
		} else {
			if prevKey != "" && prevTTL != 0 {
				fmt.Println(prevKey, prevTTL, cnt)
			}
			cnt = 0
			prevTTL = ttl
			prevKey = key
		}
	}
	if cnt != 0 {
		fmt.Println(prevKey, prevTTL, cnt)
	}
	err := iter.Close()
	if err != nil {
		log.Error(3, "cassandra query error. %s", err)
	}
}
