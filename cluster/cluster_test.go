package cluster

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestPeersForQuery(t *testing.T) {
	Init("node1", "test", time.Now())
	ThisNode.SetPrimary(true)
	ThisNode.SetPartitions([]int32{1, 2})
	ThisNode.SetReady()
	Convey("when cluster in single mode", t, func() {
		selected := PeersForQuery()
		So(selected, ShouldHaveLength, 1)
		So(selected[0], ShouldEqual, ThisNode)
	})
	mu.Lock()
	Mode = ModeMulti
	peers = append(peers, &Node{
		name:       "node2",
		primary:    true,
		partitions: []int32{1, 2},
		state:      NodeReady,
	})
	peers = append(peers, &Node{
		name:       "node3",
		primary:    true,
		partitions: []int32{3, 4},
		state:      NodeReady,
	})
	peers = append(peers, &Node{
		name:       "node4",
		primary:    true,
		partitions: []int32{3, 4},
		state:      NodeReady,
	})
	mu.Unlock()
	Convey("when cluster in multi mode", t, func() {
		selected := PeersForQuery()
		So(selected, ShouldHaveLength, 2)
		So(selected[0], ShouldEqual, ThisNode)
		Convey("peers should be selected randomly with even distribution", func() {
			peerCount := make(map[string]int)
			for i := 0; i < 1000; i++ {
				selected = PeersForQuery()
				for _, p := range selected {
					peerCount[p.GetName()]++
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
