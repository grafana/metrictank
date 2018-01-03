package cluster

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestPeersForQuerySingle(t *testing.T) {
	Mode = ModeSingle
	Init("node1", "test", time.Now(), "http", 6060)
	Manager.SetPrimary(true)
	Manager.SetPartitions([]int32{1, 2})
	maxPrio = 10
	Manager.SetPriority(10)
	Manager.SetReady()
	Convey("when cluster in single mode", t, func() {
		selected, err := MembersForQuery()
		So(err, ShouldBeNil)
		So(selected, ShouldHaveLength, 1)
		So(selected[0], ShouldResemble, Manager.ThisNode())
	})
}

func TestPeersForQueryMulti(t *testing.T) {
	Mode = ModeMulti
	Init("node1", "test", time.Now(), "http", 6060)
	manager := Manager.(*MemberlistManager)
	manager.SetPrimary(true)
	manager.SetPartitions([]int32{1, 2})
	maxPrio = 10
	manager.SetPriority(10)
	manager.SetReady()
	thisNode := manager.thisNode()
	manager.Lock()
	manager.members = map[string]HTTPNode{
		thisNode.GetName(): thisNode,
		"node2": {
			Name:       "node2",
			Primary:    true,
			Partitions: []int32{1, 2},
			State:      NodeReady,
			Priority:   10,
		},
		"node3": {
			Name:       "node3",
			Primary:    true,
			Partitions: []int32{3, 4},
			State:      NodeReady,
			Priority:   10,
		},
		"node4": {
			Name:       "node4",
			Primary:    true,
			Partitions: []int32{3, 4},
			State:      NodeReady,
			Priority:   10,
		},
	}
	manager.Unlock()
	Convey("when cluster in multi mode", t, func() {
		selected, err := MembersForQuery()
		So(err, ShouldBeNil)
		So(selected, ShouldHaveLength, 2)
		nodeNames := []string{}
		for _, n := range selected {
			nodeNames = append(nodeNames, n.GetName())
			if n.GetName() == manager.thisNode().GetName() {
				So(n, ShouldResemble, manager.thisNode())
			}
		}

		So(nodeNames, ShouldContain, manager.thisNode().GetName())
		Convey("members should be selected randomly with even distribution", func() {
			peerCount := make(map[string]int)
			for i := 0; i < 100; i++ {
				selected, err = MembersForQuery()
				So(err, ShouldBeNil)
				for _, p := range selected {
					peerCount[p.GetName()]++
				}
			}
			So(peerCount["node1"], ShouldEqual, 100)
			So(peerCount["node2"], ShouldEqual, 0)
			So(peerCount["node3"], ShouldEqual, 50)
			So(peerCount["node4"], ShouldEqual, 50)
			for p, count := range peerCount {
				t.Logf("%s: %d", p, count)
			}
		})
	})
	Convey("when shards missing", t, func() {
		minAvailableShards = 5
		selected, err := MembersForQuery()
		So(err, ShouldEqual, InsufficientShardsAvailable)
		So(selected, ShouldHaveLength, 0)
	})
}
