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
	now         uint32
}

func TestAlignRequests(t *testing.T) {
	input := []alignCase{
		{
			// a basic case with uniform raw intervals
			[]models.Req{
				reqRaw("a", 0, 30, 800, 60, consolidation.Avg),
				reqRaw("b", 0, 30, 800, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				1000, // need TTL of 1000 to read back from ts=0 if now=1000
				[]mdata.AggSetting{},
			},
			[]models.Req{
				reqOut("a", 0, 30, 800, 60, consolidation.Avg, 0, 60, 60, 1),
				reqOut("b", 0, 30, 800, 60, consolidation.Avg, 0, 60, 60, 1),
			},
			nil,
			1000,
		},
		{
			// real example seen with alerting queries
			// need to consolidate to bring them to the same step
			// because raw intervals are not uniform
			[]models.Req{
				reqRaw("a", 0, 30, 800, 10, consolidation.Avg),
				reqRaw("b", 0, 30, 800, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				1000, // need TTL of 1000 to read back from ts=0 if now=1000
				[]mdata.AggSetting{},
			},
			[]models.Req{
				reqOut("a", 0, 30, 800, 10, consolidation.Avg, 0, 10, 60, 6),
				reqOut("b", 0, 30, 800, 60, consolidation.Avg, 0, 60, 60, 1),
			},
			nil,
			1000,
		},
		{
			// same but now raw TTL is not long enough but we don't have a rollup so best effort from raw
			[]models.Req{
				reqRaw("a", 0, 30, 800, 10, consolidation.Avg),
				reqRaw("b", 0, 30, 800, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				800,
				[]mdata.AggSetting{},
			},
			[]models.Req{
				reqOut("a", 0, 30, 800, 10, consolidation.Avg, 0, 10, 60, 6),
				reqOut("b", 0, 30, 800, 60, consolidation.Avg, 0, 60, 60, 1),
			},
			nil,
			1000,
		},
		{
			// now raw TTL is not long and we have a rollup we can use instead
			[]models.Req{
				reqRaw("a", 0, 30, 800, 10, consolidation.Avg),
				reqRaw("b", 0, 30, 800, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				999, // just not long enough
				[]mdata.AggSetting{
					mdata.NewAggSetting(120, 600, 2, 1000, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 30, 800, 10, consolidation.Avg, 1, 120, 120, 1),
				reqOut("b", 0, 30, 800, 60, consolidation.Avg, 1, 120, 120, 1),
			},
			nil,
			1000,
		},
		{
			// now raw TTL is not long and we have a rollup we can use instead, at same interval as one of the raws
			// i suppose our engine could be a bit smarter and see for the 2nd one which it has more in memory of.
			[]models.Req{
				reqRaw("a", 0, 30, 800, 10, consolidation.Avg),
				reqRaw("b", 0, 30, 800, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				999, // just not long enough
				[]mdata.AggSetting{
					mdata.NewAggSetting(60, 600, 2, 1000, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 30, 800, 10, consolidation.Avg, 1, 60, 60, 1),
				reqOut("b", 0, 30, 800, 60, consolidation.Avg, 1, 60, 60, 1),
			},
			nil,
			1000,
		},
		{
			// now TTL of first rollup is *just* enough
			[]models.Req{
				reqRaw("a", 0, 30, 800, 10, consolidation.Avg),
				reqRaw("b", 0, 30, 800, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				900, // just not long enough
				[]mdata.AggSetting{
					mdata.NewAggSetting(120, 600, 2, 1000, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 30, 800, 10, consolidation.Avg, 1, 120, 120, 1),
				reqOut("b", 0, 30, 800, 60, consolidation.Avg, 1, 120, 120, 1),
			},
			nil,
			1000,
		},
		{
			// now TTL of first rollup is not enough but we have no other choice but to use it
			[]models.Req{
				reqRaw("a", 0, 30, 800, 10, consolidation.Avg),
				reqRaw("b", 0, 30, 800, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				900, // just not long enough
				[]mdata.AggSetting{
					mdata.NewAggSetting(120, 600, 2, 999, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 30, 800, 10, consolidation.Avg, 1, 120, 120, 1),
				reqOut("b", 0, 30, 800, 60, consolidation.Avg, 1, 120, 120, 1),
			},
			nil,
			1000,
		},
		{
			// now TTL of first rollup is not enough and we have a 3rd band to use
			[]models.Req{
				reqRaw("a", 0, 30, 800, 10, consolidation.Avg),
				reqRaw("b", 0, 30, 800, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				900, // just not long enough
				[]mdata.AggSetting{
					mdata.NewAggSetting(120, 600, 2, 999, true),
					mdata.NewAggSetting(240, 600, 2, 1000, true),
				},
			},
			[]models.Req{
				reqOut("a", 0, 30, 800, 10, consolidation.Avg, 2, 240, 240, 1),
				reqOut("b", 0, 30, 800, 60, consolidation.Avg, 2, 240, 240, 1),
			},
			nil,
			1000,
		},
		{
			// now TTL of raw/first rollup is not enough but the two rollups are disabled, so must use raw
			[]models.Req{
				reqRaw("a", 0, 30, 800, 10, consolidation.Avg),
				reqRaw("b", 0, 30, 800, 60, consolidation.Avg),
			},
			mdata.AggSettings{
				900, // just not long enough
				[]mdata.AggSetting{
					mdata.NewAggSetting(120, 600, 2, 999, false),
					mdata.NewAggSetting(240, 600, 2, 1000, false),
				},
			},
			[]models.Req{
				reqOut("a", 0, 30, 800, 10, consolidation.Avg, 0, 10, 60, 6),
				reqOut("b", 0, 30, 800, 60, consolidation.Avg, 0, 60, 60, 1),
			},
			nil,
			1000,
		},
	}
	for i, ac := range input {
		out, err := alignRequests(ac.now, ac.reqs, ac.aggSettings)
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

func BenchmarkAlignRequests(b *testing.B) {
	var res []models.Req
	reqs := []models.Req{
		reqRaw("a", 0, 3600*24*7, 1000, 10, consolidation.Avg),
		reqRaw("b", 0, 3600*24*7, 1000, 30, consolidation.Avg),
		reqRaw("c", 0, 3600*24*7, 1000, 60, consolidation.Avg),
	}
	aggSettings := mdata.AggSettings{
		35 * 24 * 60 * 60,
		[]mdata.AggSetting{
			mdata.NewAggSetting(600, 21600, 1, 2*30*24*3600, true),
			mdata.NewAggSetting(7200, 21600, 1, 6*30*24*3600, true),
			mdata.NewAggSetting(21600, 21600, 1, 2*365*24*3600, true),
		},
	}

	for n := 0; n < b.N; n++ {
		res, _ = alignRequests(14*24*3600, reqs, aggSettings)
	}
	result = res
}
