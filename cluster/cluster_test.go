package cluster

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestPeersForQuery(t *testing.T) {
	Init("node1", "test", time.Now(), "http", 6060)
	Manager.SetPrimary(true)
	Manager.SetPartitions([]int32{1, 2})
	Manager.SetReady()
	Convey("when cluster in single mode", t, func() {
		selected := MembersForQuery()
		So(selected, ShouldHaveLength, 1)
		So(selected[0], ShouldResemble, Manager.ThisNode())
	})
	thisNode := Manager.ThisNode()
	Manager.Lock()
	Mode = ModeMulti
	Manager.members = map[string]Node{
		thisNode.Name: thisNode,
		"node2": {
			Name:       "node2",
			Primary:    true,
			Partitions: []int32{1, 2},
			State:      NodeReady,
		},
		"node3": {
			Name:       "node3",
			Primary:    true,
			Partitions: []int32{3, 4},
			State:      NodeReady,
		},
		"node4": {
			Name:       "node4",
			Primary:    true,
			Partitions: []int32{3, 4},
			State:      NodeReady,
		},
	}
	Manager.Unlock()
	Convey("when cluster in multi mode", t, func() {
		selected := MembersForQuery()
		So(selected, ShouldHaveLength, 2)
		nodeNames := []string{}
		for _, n := range selected {
			nodeNames = append(nodeNames, n.Name)
			if n.Name == Manager.ThisNode().Name {
				So(n, ShouldResemble, Manager.ThisNode())
			}
		}

		So(nodeNames, ShouldContain, Manager.ThisNode().Name)
		Convey("members should be selected randomly with even distribution", func() {
			peerCount := make(map[string]int)
			for i := 0; i < 1000; i++ {
				selected = MembersForQuery()
				for _, p := range selected {
					peerCount[p.Name]++
				}
			}
			So(peerCount["node1"], ShouldEqual, 1000)
			So(peerCount["node2"], ShouldEqual, 0)
			So(peerCount["node3"], ShouldNotAlmostEqual, 500)
			So(peerCount["node4"], ShouldNotAlmostEqual, 500)
			for p, count := range peerCount {
				t.Logf("%s: %d", p, count)
			}
		})
	})

}
