package cluster

import (
	"time"
)

var ThisNode *Node

func Init(name, version string, primary bool, started time.Time) {
	ThisNode = &Node{
		name:          name,
		started:       started,
		version:       version,
		primary:       primary,
		primaryChange: time.Now(),
		stateChange:   time.Now(),
	}
}
