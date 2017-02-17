package api

import (
	"testing"

	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/mdata"
)

type alignCase struct {
	reqs        []models.Req
	aggSettings mdata.AggSettings
	outReqs     []models.Req
	outErr      error
}

func TestAlignRequestsLegacy(t *testing.T) {
	input := []alignCase{
		{
			// real example seen with alerting queries
			[]models.Req{
				reqRaw("a", 0, 30, 800, 10, consolidation.Avg),
				reqRaw("b", 0, 30, 800, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{},
			},
			[]models.Req{
				reqOut("a", 0, 30, 800, 10, consolidation.Avg, 0, 10, 60, 6),
				reqOut("b", 0, 30, 800, 60, consolidation.Avg, 0, 60, 60, 1),
			},
			nil,
		},
		{
			// raw would be 3600/10=360 points, agg1 3600/60=60. raw is best cause it provides most points
			// and still under the max points limit.
			[]models.Req{
				reqRaw("a", 0, 3600, 800, 10, consolidation.Avg),
				reqRaw("b", 0, 3600, 800, 10, consolidation.Avg),
				reqRaw("c", 0, 3600, 800, 10, consolidation.Avg),
			},
			// span, chunkspan, numchunks, ttl, ready
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(60, 600, 2, 0, true),
					mdata.NewAggSetting(120, 600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600, 800, 10, consolidation.Avg, 0, 10, 10, 1),
				reqOut("b", 0, 3600, 800, 10, consolidation.Avg, 0, 10, 10, 1),
				reqOut("c", 0, 3600, 800, 10, consolidation.Avg, 0, 10, 10, 1),
			},
			nil,
		},
		{
			// same as before, but agg1 disabled. just to make sure it still behaves the same.
			[]models.Req{
				reqRaw("a", 0, 3600, 800, 10, consolidation.Avg),
				reqRaw("b", 0, 3600, 800, 10, consolidation.Avg),
				reqRaw("c", 0, 3600, 800, 10, consolidation.Avg),
			},
			// span, chunkspan, numchunks, ttl, ready
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(60, 600, 2, 0, false),
					mdata.NewAggSetting(120, 600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600, 800, 10, consolidation.Avg, 0, 10, 10, 1),
				reqOut("b", 0, 3600, 800, 10, consolidation.Avg, 0, 10, 10, 1),
				reqOut("c", 0, 3600, 800, 10, consolidation.Avg, 0, 10, 10, 1),
			},
			nil,
		},
		{
			// now we request 0-2400, with max datapoints 100.
			// raw: 2400/10 -> 240pts -> needs runtime consolidation
			// agg1: 2400/60 -> 40 pts, good candidate,
			// though 40pts is 2.5x smaller than the maxPoints target (100 points)
			// raw's 240 pts is only 2.4x larger then 100pts, so it is selected.
			[]models.Req{
				reqRaw("a", 0, 2400, 100, 10, consolidation.Avg),
				reqRaw("b", 0, 2400, 100, 10, consolidation.Avg),
				reqRaw("c", 0, 2400, 100, 10, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(60, 600, 2, 0, true),
					mdata.NewAggSetting(120, 600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 2400, 100, 10, consolidation.Avg, 0, 10, 30, 3),
				reqOut("b", 0, 2400, 100, 10, consolidation.Avg, 0, 10, 30, 3),
				reqOut("c", 0, 2400, 100, 10, consolidation.Avg, 0, 10, 30, 3),
			},
			nil,
		},
		// same thing as above, but now we set max points to 39. So now the 240pts
		// raw: 2400/10 -> 240 pts is 6.15x our target of 39pts
		// agg1 2400/120 -> 20 pts is only 1.95x smaller then our target 39pts so it is
		// selected.
		{
			[]models.Req{
				reqRaw("a", 0, 2400, 39, 10, consolidation.Avg),
				reqRaw("b", 0, 2400, 39, 10, consolidation.Avg),
				reqRaw("c", 0, 2400, 39, 10, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(120, 600, 2, 0, true),
					mdata.NewAggSetting(600, 600, 2, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 2400, 39, 10, consolidation.Avg, 1, 120, 120, 1),
				reqOut("b", 0, 2400, 39, 10, consolidation.Avg, 1, 120, 120, 1),
				reqOut("c", 0, 2400, 39, 10, consolidation.Avg, 1, 120, 120, 1),
			},
			nil,
		},
		// same as above, except now the 120s band is disabled.
		// raw: 2400/10 -> 240 pts is 6.15x our target of 39pts
		// agg1 2400/600 -> 4 pts is about 10x smaller so we prefer raw again
		{
			[]models.Req{
				reqRaw("a", 0, 2400, 39, 10, consolidation.Avg),
				reqRaw("b", 0, 2400, 39, 10, consolidation.Avg),
				reqRaw("c", 0, 2400, 39, 10, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(120, 600, 2, 0, false),
					mdata.NewAggSetting(600, 600, 2, 0, true),
				},
			},
			// rawInterval, archive, archInterval, outInterval, aggNum
			[]models.Req{
				reqOut("a", 0, 2400, 39, 10, consolidation.Avg, 0, 10, 70, 7),
				reqOut("b", 0, 2400, 39, 10, consolidation.Avg, 0, 10, 70, 7),
				reqOut("c", 0, 2400, 39, 10, consolidation.Avg, 0, 10, 70, 7),
			},
			nil,
		},
		// same as above, 120s band is still disabled. but we query for a longer range
		// raw: 24000/10 -> 2400 pts is 61.5x our target of 39pts
		// agg2 24000/600 -> 40 pts is just too large, but we can make it work with runtime
		// consolidation
		{
			[]models.Req{
				reqRaw("a", 0, 24000, 39, 10, consolidation.Avg),
				reqRaw("b", 0, 24000, 39, 10, consolidation.Avg),
				reqRaw("c", 0, 24000, 39, 10, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(120, 600, 2, 0, false),
					mdata.NewAggSetting(600, 600, 2, 0, true),
				},
			},
			// rawInterval, archive, archInterval, outInterval, aggNum
			[]models.Req{
				reqOut("a", 0, 24000, 39, 10, consolidation.Avg, 2, 600, 1200, 2),
				reqOut("b", 0, 24000, 39, 10, consolidation.Avg, 2, 600, 1200, 2),
				reqOut("c", 0, 24000, 39, 10, consolidation.Avg, 2, 600, 1200, 2),
			},
			nil,
		},
		// now something a bit different. 3 different raw intervals, but same aggregation settings.
		// raw is here best again but all series need to be at a step of 60
		// so runtime consolidation is needed, we'll get 40 points for each metric
		{
			[]models.Req{
				reqRaw("a", 0, 2400, 100, 10, consolidation.Avg),
				reqRaw("b", 0, 2400, 100, 30, consolidation.Avg),
				reqRaw("c", 0, 2400, 100, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(120, 600, 2, 0, true),
					mdata.NewAggSetting(600, 600, 2, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 2400, 100, 10, consolidation.Avg, 0, 10, 60, 6),
				reqOut("b", 0, 2400, 100, 30, consolidation.Avg, 0, 30, 60, 2),
				reqOut("c", 0, 2400, 100, 60, consolidation.Avg, 0, 60, 60, 1),
			},
			nil,
		},

		// Similar to above with 3 different raw intervals, but these raw intervals
		// require a little more calculation to get the minimum interval they all fit into.
		// because the minimum interval that they all fit into (300) is greater then the
		// 120second rollup data, the rollups is a better choice.
		{
			[]models.Req{
				reqRaw("a", 0, 2400, 100, 10, consolidation.Avg),
				reqRaw("b", 0, 2400, 100, 50, consolidation.Avg),
				reqRaw("c", 0, 2400, 100, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(120, 600, 2, 0, true),
					mdata.NewAggSetting(600, 600, 2, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 2400, 100, 10, consolidation.Avg, 1, 120, 120, 1),
				reqOut("b", 0, 2400, 100, 50, consolidation.Avg, 1, 120, 120, 1),
				reqOut("c", 0, 2400, 100, 60, consolidation.Avg, 1, 120, 120, 1),
			},
			nil,
		},
		// again with 3 different raw intervals that have a large common interval.
		// With this test, our common raw interval matches our first rollup. Runtime consolidation is expensive
		// so we preference the rollup data.
		{
			[]models.Req{
				reqRaw("a", 0, 2400, 100, 10, consolidation.Avg),
				reqRaw("b", 0, 2400, 100, 50, consolidation.Avg),
				reqRaw("c", 0, 2400, 100, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(300, 600, 2, 0, true),
					mdata.NewAggSetting(600, 600, 2, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 2400, 100, 10, consolidation.Avg, 1, 300, 300, 1),
				reqOut("b", 0, 2400, 100, 50, consolidation.Avg, 1, 300, 300, 1),
				reqOut("c", 0, 2400, 100, 60, consolidation.Avg, 1, 300, 300, 1),
			},
			nil,
		},
		// same but now the first rollup band is disabled
		// raw 2400/300 -> 8 points
		// agg 2 2400/600 -> 4 points
		// best use raw despite the needed consolidation

		{
			[]models.Req{
				reqRaw("a", 0, 2400, 100, 10, consolidation.Avg),
				reqRaw("b", 0, 2400, 100, 50, consolidation.Avg),
				reqRaw("c", 0, 2400, 100, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(300, 600, 2, 0, false),
					mdata.NewAggSetting(600, 600, 2, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 2400, 100, 10, consolidation.Avg, 0, 10, 300, 30),
				reqOut("b", 0, 2400, 100, 50, consolidation.Avg, 0, 50, 300, 6),
				reqOut("c", 0, 2400, 100, 60, consolidation.Avg, 0, 60, 300, 5),
			},
			nil,
		},
		// again with 3 different raw intervals that have a large common interval.
		// With this test, our common raw interval is less then our first rollup so is selected.
		{
			[]models.Req{
				reqRaw("a", 0, 2400, 100, 10, consolidation.Avg),
				reqRaw("b", 0, 2400, 100, 50, consolidation.Avg),
				reqRaw("c", 0, 2400, 100, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(600, 600, 2, 0, true),
					mdata.NewAggSetting(1200, 1200, 2, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 2400, 100, 10, consolidation.Avg, 0, 10, 300, 30),
				reqOut("b", 0, 2400, 100, 50, consolidation.Avg, 0, 50, 300, 6),
				reqOut("c", 0, 2400, 100, 60, consolidation.Avg, 0, 60, 300, 5),
			},
			nil,
		},
		// let's do a realistic one: request 3h worth of data
		// raw means an alignment at 60s interval so:
		// raw -> 10800/60 -> 180 points
		// clearly this fits well within the max points, and it's the highest res,
		// so it's returned.
		{
			[]models.Req{
				reqRaw("a", 0, 3600*3, 1000, 10, consolidation.Avg),
				reqRaw("b", 0, 3600*3, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*3, 1000, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(600, 21600, 1, 0, true), // aggregations stored in 6h chunks
					mdata.NewAggSetting(7200, 21600, 1, 0, true),
					mdata.NewAggSetting(21600, 21600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600*3, 1000, 10, consolidation.Avg, 0, 10, 60, 6),
				reqOut("b", 0, 3600*3, 1000, 30, consolidation.Avg, 0, 30, 60, 2),
				reqOut("c", 0, 3600*3, 1000, 60, consolidation.Avg, 0, 60, 60, 1),
			},
			nil,
		},
		// same but request 6h worth of data
		// raw 21600/60 -> 360. chosen for same reason
		{
			[]models.Req{
				reqRaw("a", 0, 3600*6, 1000, 10, consolidation.Avg),
				reqRaw("b", 0, 3600*6, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*6, 1000, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(600, 21600, 1, 0, true), // aggregations stored in 6h chunks
					mdata.NewAggSetting(7200, 21600, 1, 0, true),
					mdata.NewAggSetting(21600, 21600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600*6, 1000, 10, consolidation.Avg, 0, 10, 60, 6),
				reqOut("b", 0, 3600*6, 1000, 30, consolidation.Avg, 0, 30, 60, 2),
				reqOut("c", 0, 3600*6, 1000, 60, consolidation.Avg, 0, 60, 60, 1),
			},
			nil,
		},
		// same but request 9h worth of data
		// raw 32400/60 -> 540. chosen for same reason
		{
			[]models.Req{
				reqRaw("a", 0, 3600*9, 1000, 10, consolidation.Avg),
				reqRaw("b", 0, 3600*9, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*9, 1000, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(600, 21600, 1, 0, true), // aggregations stored in 6h chunks
					mdata.NewAggSetting(7200, 21600, 1, 0, true),
					mdata.NewAggSetting(21600, 21600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600*9, 1000, 10, consolidation.Avg, 0, 10, 60, 6),
				reqOut("b", 0, 3600*9, 1000, 30, consolidation.Avg, 0, 30, 60, 2),
				reqOut("c", 0, 3600*9, 1000, 60, consolidation.Avg, 0, 60, 60, 1),
			},
			nil,
		},
		// same but request 24h worth of data
		// raw 86400/60 -> 1440
		// agg1 86400/600 -> 144 points -> best choice
		{
			[]models.Req{
				reqRaw("a", 0, 3600*24, 1000, 10, consolidation.Avg),
				reqRaw("b", 0, 3600*24, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*24, 1000, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(600, 21600, 1, 0, true), // aggregations stored in 6h chunks
					mdata.NewAggSetting(7200, 21600, 1, 0, true),
					mdata.NewAggSetting(21600, 21600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600*24, 1000, 10, consolidation.Avg, 1, 600, 600, 1),
				reqOut("b", 0, 3600*24, 1000, 30, consolidation.Avg, 1, 600, 600, 1),
				reqOut("c", 0, 3600*24, 1000, 60, consolidation.Avg, 1, 600, 600, 1),
			},
			nil,
		},
		// same but now let's request 2 weeks worth of data.
		// not using raw is a no brainer.
		// agg1 3600*24*7 / 600 = 1008 points, which is too many, so must also do runtime consolidation and bring it back to 504
		// agg2 3600*24*7 / 7200 = 84 points -> too far below maxdatapoints, better to do agg1 with runtime consol
		{
			[]models.Req{
				reqRaw("a", 0, 3600*24*7, 1000, 10, consolidation.Avg),
				reqRaw("b", 0, 3600*24*7, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*24*7, 1000, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(600, 21600, 1, 0, true), // aggregations stored in 6h chunks
					mdata.NewAggSetting(7200, 21600, 1, 0, true),
					mdata.NewAggSetting(21600, 21600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600*24*7, 1000, 10, consolidation.Avg, 1, 600, 1200, 2),
				reqOut("b", 0, 3600*24*7, 1000, 30, consolidation.Avg, 1, 600, 1200, 2),
				reqOut("c", 0, 3600*24*7, 1000, 60, consolidation.Avg, 1, 600, 1200, 2),
			},
			nil,
		},
		// let's request 1 year of data
		// raw 3600*24*365/60 -> 525600
		// agg1 3600*24*365/600 -> 52560
		// agg2 3600*24*365/7200 -> 4380
		// agg3 3600*24*365/21600 -> 1460
		// clearly agg3 is the best, and we have to runtime consolidate with aggNum 2
		{
			[]models.Req{
				reqRaw("a", 0, 3600*24*365, 1000, 10, consolidation.Avg),
				reqRaw("b", 0, 3600*24*365, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*24*365, 1000, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(600, 21600, 1, 0, true), // aggregations stored in 6h chunks
					mdata.NewAggSetting(7200, 21600, 1, 0, true),
					mdata.NewAggSetting(21600, 21600, 1, 0, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600*24*365, 1000, 10, consolidation.Avg, 3, 21600, 43200, 2),
				reqOut("b", 0, 3600*24*365, 1000, 30, consolidation.Avg, 3, 21600, 43200, 2),
				reqOut("c", 0, 3600*24*365, 1000, 60, consolidation.Avg, 3, 21600, 43200, 2),
			},
			nil,
		},
		// ditto but disable agg3
		// so we have to use agg2 with aggNum of 5
		{
			[]models.Req{
				reqRaw("a", 0, 3600*24*365, 1000, 10, consolidation.Avg),
				reqRaw("b", 0, 3600*24*365, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*24*365, 1000, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{
					mdata.NewAggSetting(600, 21600, 1, 0, true), // aggregations stored in 6h chunks
					mdata.NewAggSetting(7200, 21600, 1, 0, true),
					mdata.NewAggSetting(21600, 21600, 1, 0, false),
				},
			},
			[]models.Req{
				reqOut("a", 0, 3600*24*365, 1000, 10, consolidation.Avg, 2, 7200, 36000, 5),
				reqOut("b", 0, 3600*24*365, 1000, 30, consolidation.Avg, 2, 7200, 36000, 5),
				reqOut("c", 0, 3600*24*365, 1000, 60, consolidation.Avg, 2, 7200, 36000, 5),
			},
			nil,
		},
		// now let's request 1 year of data again, but without actually having any aggregation bands (wowa don't do this)
		// raw 3600*24*365/60 -> 525600
		// we need an aggNum of 526 to keep this under 1000 points
		{
			[]models.Req{
				reqRaw("a", 0, 3600*24*365, 1000, 10, consolidation.Avg),
				reqRaw("b", 0, 3600*24*365, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*24*365, 1000, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{},
			},
			[]models.Req{
				reqOut("a", 0, 3600*24*365, 1000, 10, consolidation.Avg, 0, 10, 31560, 526*6),
				reqOut("b", 0, 3600*24*365, 1000, 30, consolidation.Avg, 0, 30, 31560, 526*2),
				reqOut("c", 0, 3600*24*365, 1000, 60, consolidation.Avg, 0, 60, 31560, 526),
			},
			nil,
		},
		// same thing but if the metrics have the same resolution
		// raw 3600*24*365/60 -> 525600
		// we need an aggNum of 526 to keep this under 1000 points
		{
			[]models.Req{
				reqRaw("a", 0, 3600*24*365, 1000, 30, consolidation.Avg),
				reqRaw("b", 0, 3600*24*365, 1000, 30, consolidation.Avg),
				reqRaw("c", 0, 3600*24*365, 1000, 30, consolidation.Avg),
			},
			mdata.AggSettings{
				35 * 24 * 60 * 60,
				[]mdata.AggSetting{},
			},
			[]models.Req{
				reqOut("a", 0, 3600*24*365, 1000, 30, consolidation.Avg, 0, 30, 31560, 526*2),
				reqOut("b", 0, 3600*24*365, 1000, 30, consolidation.Avg, 0, 30, 31560, 526*2),
				reqOut("c", 0, 3600*24*365, 1000, 30, consolidation.Avg, 0, 30, 31560, 526*2),
			},
			nil,
		},
	}
	for i, ac := range input {
		out, err := alignRequestsLegacy(ac.reqs, ac.aggSettings)
		if err != ac.outErr {
			t.Errorf("different err value for testcase %d  expected: %v, got: %v", i, ac.outErr, err)
		}
		if len(out) != len(ac.outReqs) {
			t.Errorf("different number of requests for testcase %d  expected: %v, got: %v", i, len(ac.outReqs), len(out))
		} else {
			for r, exp := range ac.outReqs {
				if !compareReqEqual(exp, out[r]) {
					t.Errorf("testcase %d, request %d:\nexpected: %v\n     got: %v", i, r, exp.DebugString(), out[r].DebugString())
				}
			}
		}
	}
}

var result []models.Req

func BenchmarkAlignRequestsLegacy(b *testing.B) {
	var res []models.Req
	reqs := []models.Req{
		reqRaw("a", 0, 3600*24*7, 1000, 10, consolidation.Avg),
		reqRaw("b", 0, 3600*24*7, 1000, 30, consolidation.Avg),
		reqRaw("c", 0, 3600*24*7, 1000, 60, consolidation.Avg),
	}
	aggSettings := mdata.AggSettings{
		35 * 24 * 60 * 60,
		[]mdata.AggSetting{
			mdata.NewAggSetting(600, 21600, 1, 0, true),
			mdata.NewAggSetting(7200, 21600, 1, 0, true),
			mdata.NewAggSetting(21600, 21600, 1, 0, true),
		},
	}

	for n := 0; n < b.N; n++ {
		res, _ = alignRequestsLegacy(reqs, aggSettings)
	}
	result = res
}
