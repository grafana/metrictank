package mdata

// this file is for clustering, in particular to talk to instances who host other data (shards)

import (
	"io/ioutil"
	"net/http"

	"github.com/Unknwon/log"
)

func TryNodes(nodes []string) {
	for _, node := range nodes {
		res, err := http.Get(node)
		if err != nil {
			log.Warn("failed to query other instance %q: %s", node, err)
		}
		body, err := ioutil.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			log.Warn("CLU failed to read body from other instance %q: %s", node, err)
		}
		// TODO how to handle other instances that are still warming up? maybe invalidate and periodic retry?
		if string(body) != "OK" {
			log.Warn("CLU other instance %q does not seem ready. got response %q", node, body)
		}
	}
}
