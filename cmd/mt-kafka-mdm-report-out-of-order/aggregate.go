package main

import (
	"strings"

	log "github.com/sirupsen/logrus"
)

type Aggregate struct {
	Count           int
	OutOfOrderCount int
	DuplicateCount  int
}

func aggregateByName(tracker Tracker) map[string]Aggregate {
	aggregates := map[string]Aggregate{}

	for _, track := range tracker {
		aggregate, _ := aggregates[track.Name]
		aggregate.Count += track.Count
		aggregate.OutOfOrderCount += track.OutOfOrderCount
		aggregate.DuplicateCount += track.DuplicateCount
		aggregates[track.Name] = aggregate
	}

	return aggregates
}

func aggregateByTag(tracker Tracker, groupByTag string) map[string]Aggregate {
	aggregates := map[string]Aggregate{}

	for _, track := range tracker {
		for _, tag := range track.Tags {
			kv := strings.Split(tag, "=")
			if len(kv) != 2 {
				log.Errorf("unexpected tag encoding for metric with name=%q tag=%q", track.Name, tag)
				continue
			}
			if kv[0] == groupByTag {
				aggregate, _ := aggregates[kv[1]]
				aggregate.Count += track.Count
				aggregate.OutOfOrderCount += track.OutOfOrderCount
				aggregate.DuplicateCount += track.DuplicateCount
				aggregates[kv[1]] = aggregate
			}
		}
	}

	return aggregates
}
