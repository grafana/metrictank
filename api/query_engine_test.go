package api

import (
	"regexp"
	"testing"

	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/conf"
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/mdata"
)

// testAlign verifies the aligment of the given requests, given the retentions (one or more patterns, one or more retentions each)
func testAlign(reqs []models.Req, retentions [][]conf.Retention, outReqs []models.Req, outErr error, now uint32, t *testing.T) {
	var schemas []conf.Schema
	for _, ret := range retentions {
		schemas = append(schemas, conf.Schema{
			Pattern:    regexp.MustCompile(".*"),
			Retentions: conf.Retentions(ret),
		})
	}

	mdata.Schemas = conf.Schemas(schemas)
	out, err := alignRequests(now, reqs)
	if err != outErr {
		t.Errorf("different err value expected: %v, got: %v", outErr, err)
	}
	if len(out) != len(outReqs) {
		t.Errorf("different number of requests expected: %v, got: %v", len(outReqs), len(out))
	} else {
		for r, exp := range outReqs {
			if !compareReqEqual(exp, out[r]) {
				t.Errorf("request %d:\nexpected: %v\n     got: %v", r, exp.DebugString(), out[r].DebugString())
			}
		}
	}
}

// 2 series requested with equal raw intervals. req 0-30. now 1200. one archive of ttl=1200 does it
func TestAlignRequestsBasic(t *testing.T) {
	testAlign([]models.Req{
		reqRaw("a", 0, 30, 800, 60, consolidation.Avg, 0, 0),
		reqRaw("b", 0, 30, 800, 60, consolidation.Avg, 0, 0),
	},
		[][]conf.Retention{
			{
				conf.NewRetentionMT(60, 1200, 0, 0, true),
			},
		},
		[]models.Req{
			reqOut("a", 0, 30, 800, 60, consolidation.Avg, 0, 0, 0, 60, 1200, 60, 1),
			reqOut("b", 0, 30, 800, 60, consolidation.Avg, 0, 0, 0, 60, 1200, 60, 1),
		},
		nil,
		1200,
		t,
	)
}

// 2 series requested with equal raw intervals from different schemas. req 0-30. now 1200. their archives of ttl=1200 do it
func TestAlignRequestsBasicDiff(t *testing.T) {
	testAlign([]models.Req{
		reqRaw("a", 0, 30, 800, 60, consolidation.Avg, 0, 0),
		reqRaw("b", 0, 30, 800, 60, consolidation.Avg, 1, 0),
	},
		[][]conf.Retention{
			{
				conf.NewRetentionMT(60, 1200, 0, 0, true),
			},
			{
				conf.NewRetentionMT(60, 1200, 0, 0, true),
			},
		},
		[]models.Req{
			reqOut("a", 0, 30, 800, 60, consolidation.Avg, 0, 0, 0, 60, 1200, 60, 1),
			reqOut("b", 0, 30, 800, 60, consolidation.Avg, 1, 0, 0, 60, 1200, 60, 1),
		},
		nil,
		1200,
		t,
	)
}

// 2 series requested with different raw intervals from different schemas. req 0-30. now 1200. their archives of ttl=1200 do it, but needs normalizing
// (real example seen with alerting queries)
func TestAlignRequestsAlerting(t *testing.T) {
	testAlign([]models.Req{
		reqRaw("a", 0, 30, 800, 10, consolidation.Avg, 0, 0),
		reqRaw("b", 0, 30, 800, 60, consolidation.Avg, 1, 0),
	},
		[][]conf.Retention{{
			conf.NewRetentionMT(10, 1200, 0, 0, true),
		}, {
			conf.NewRetentionMT(60, 1200, 0, 0, true),
		},
		},
		[]models.Req{
			reqOut("a", 0, 30, 800, 10, consolidation.Avg, 0, 0, 0, 10, 1200, 60, 6),
			reqOut("b", 0, 30, 800, 60, consolidation.Avg, 1, 0, 0, 60, 1200, 60, 1),
		},
		nil,
		1200,
		t,
	)
}

// 2 series requested with different raw intervals from different schemas. req 0-30. now 1200. neither has long enough archive. no rollups, so best effort from raw
func TestAlignRequestsBasicBestEffort(t *testing.T) {
	testAlign([]models.Req{
		reqRaw("a", 0, 30, 800, 10, consolidation.Avg, 0, 0),
		reqRaw("b", 0, 30, 800, 60, consolidation.Avg, 1, 0),
	},
		[][]conf.Retention{
			{
				conf.NewRetentionMT(10, 800, 0, 0, true),
			}, {
				conf.NewRetentionMT(60, 1100, 0, 0, true),
			},
		},
		[]models.Req{
			reqOut("a", 0, 30, 800, 10, consolidation.Avg, 0, 0, 0, 10, 800, 60, 6),
			reqOut("b", 0, 30, 800, 60, consolidation.Avg, 1, 0, 0, 60, 1100, 60, 1),
		},
		nil,
		1200,
		t,
	)
}

// 2 series requested with different raw intervals from different schemas. req 0-30. now 1200. one has short raw. other has short raw + good rollup
func TestAlignRequestsHalfGood(t *testing.T) {
	testAlign([]models.Req{
		reqRaw("a", 0, 30, 800, 10, consolidation.Avg, 0, 0),
		reqRaw("b", 0, 30, 800, 60, consolidation.Avg, 1, 0),
	},
		[][]conf.Retention{
			{
				conf.NewRetentionMT(10, 800, 0, 0, true),
			}, {
				conf.NewRetentionMT(60, 1100, 0, 0, true),
				conf.NewRetentionMT(120, 1200, 0, 0, true),
			},
		},
		[]models.Req{
			reqOut("a", 0, 30, 800, 10, consolidation.Avg, 0, 0, 0, 10, 800, 120, 12),
			reqOut("b", 0, 30, 800, 60, consolidation.Avg, 1, 0, 1, 120, 1200, 120, 1),
		},
		nil,
		1200,
		t,
	)
}

// 2 series requested with different raw intervals from different schemas. req 0-30. now 1200. both have short raw + good rollup
func TestAlignRequestsGoodRollup(t *testing.T) {
	testAlign([]models.Req{
		reqRaw("a", 0, 30, 800, 10, consolidation.Avg, 0, 0),
		reqRaw("b", 0, 30, 800, 60, consolidation.Avg, 1, 0),
	},
		[][]conf.Retention{
			{
				conf.NewRetentionMT(10, 1199, 0, 0, true), // just not long enough
				conf.NewRetentionMT(120, 1200, 600, 2, true),
			},
			{
				conf.NewRetentionMT(60, 1199, 0, 0, true), // just not long enough
				conf.NewRetentionMT(120, 1200, 600, 2, true),
			},
		},
		[]models.Req{
			reqOut("a", 0, 30, 800, 10, consolidation.Avg, 0, 0, 1, 120, 1200, 120, 1),
			reqOut("b", 0, 30, 800, 60, consolidation.Avg, 1, 0, 1, 120, 1200, 120, 1),
		},
		nil,
		1200,
		t,
	)
}

// 2 series requested with different raw intervals, and rollup intervals from different schemas. req 0-30. now 1200. both have short raw + good rollup
func TestAlignRequestsDiffGoodRollup(t *testing.T) {
	testAlign([]models.Req{
		reqRaw("a", 0, 30, 800, 10, consolidation.Avg, 0, 0),
		reqRaw("b", 0, 30, 800, 60, consolidation.Avg, 1, 0),
	},
		[][]conf.Retention{
			{
				conf.NewRetentionMT(10, 1199, 0, 0, true), // just not long enough
				conf.NewRetentionMT(100, 1200, 600, 2, true),
			},
			{
				conf.NewRetentionMT(60, 1199, 0, 0, true), // just not long enough
				conf.NewRetentionMT(600, 1200, 600, 2, true),
			},
		},
		[]models.Req{
			reqOut("a", 0, 30, 800, 10, consolidation.Avg, 0, 0, 1, 100, 1200, 600, 6),
			reqOut("b", 0, 30, 800, 60, consolidation.Avg, 1, 0, 1, 600, 1200, 600, 1),
		},
		nil,
		1200,
		t,
	)
}

// now raw is short and we have a rollup we can use instead, at same interval as one of the raws
func TestAlignRequestsWeird(t *testing.T) {
	testAlign([]models.Req{
		reqRaw("a", 0, 30, 800, 10, consolidation.Avg, 0, 0),
		reqRaw("b", 0, 30, 800, 60, consolidation.Avg, 1, 0),
	},
		[][]conf.Retention{
			{
				conf.NewRetentionMT(10, 1199, 0, 0, true),
				conf.NewRetentionMT(60, 1200, 600, 2, true),
			},
			{
				conf.NewRetentionMT(60, 1200, 0, 0, true),
			},
		},
		[]models.Req{
			reqOut("a", 0, 30, 800, 10, consolidation.Avg, 0, 0, 1, 60, 1200, 60, 1),
			reqOut("b", 0, 30, 800, 60, consolidation.Avg, 1, 0, 0, 60, 1200, 60, 1),
		},
		nil,
		1200,
		t,
	)
}

// now TTL of first rollup is *just* enough
func TestAlignRequestsWeird2(t *testing.T) {
	testAlign([]models.Req{
		reqRaw("a", 0, 30, 800, 10, consolidation.Avg, 0, 0),
		reqRaw("b", 0, 30, 800, 60, consolidation.Avg, 1, 0),
	},
		[][]conf.Retention{
			{
				conf.NewRetentionMT(10, 1100, 0, 0, true), // just not long enough
				conf.NewRetentionMT(120, 1200, 600, 2, true),
			},
			{
				conf.NewRetentionMT(60, 1100, 0, 0, true), // just not long enough
				conf.NewRetentionMT(120, 1200, 600, 2, true),
			},
		},
		[]models.Req{
			reqOut("a", 0, 30, 800, 10, consolidation.Avg, 0, 0, 1, 120, 1200, 120, 1),
			reqOut("b", 0, 30, 800, 60, consolidation.Avg, 1, 0, 1, 120, 1200, 120, 1),
		},
		nil,
		1200,
		t,
	)
}

// now TTL of first rollup is not enough but we have no other choice but to use it
func TestAlignRequestsNoOtherChoice(t *testing.T) {
	testAlign([]models.Req{
		reqRaw("a", 0, 30, 800, 10, consolidation.Avg, 0, 0),
		reqRaw("b", 0, 30, 800, 60, consolidation.Avg, 1, 0),
	},
		[][]conf.Retention{
			{
				conf.NewRetentionMT(10, 1100, 0, 0, true),
				conf.NewRetentionMT(120, 1199, 600, 2, true),
			},
			{
				conf.NewRetentionMT(60, 1100, 0, 0, true),
				conf.NewRetentionMT(120, 1199, 600, 2, true),
			},
		},
		[]models.Req{
			reqOut("a", 0, 30, 800, 10, consolidation.Avg, 0, 0, 1, 120, 1199, 120, 1),
			reqOut("b", 0, 30, 800, 60, consolidation.Avg, 1, 0, 1, 120, 1199, 120, 1),
		},
		nil,
		1200,
		t,
	)
}

// now TTL of first rollup is not enough and we have a 3rd band to use
func TestAlignRequests3rdBand(t *testing.T) {
	testAlign([]models.Req{
		reqRaw("a", 0, 30, 800, 10, consolidation.Avg, 0, 0),
		reqRaw("b", 0, 30, 800, 60, consolidation.Avg, 1, 0),
	},
		[][]conf.Retention{
			{
				conf.NewRetentionMT(1, 1100, 0, 0, true),
				conf.NewRetentionMT(120, 1199, 600, 2, true),
				conf.NewRetentionMT(240, 1200, 600, 2, true),
			},
			{
				conf.NewRetentionMT(60, 1100, 0, 0, true),
				conf.NewRetentionMT(240, 1200, 600, 2, true),
			},
		},
		[]models.Req{
			reqOut("a", 0, 30, 800, 10, consolidation.Avg, 0, 0, 2, 240, 1200, 240, 1),
			reqOut("b", 0, 30, 800, 60, consolidation.Avg, 1, 0, 1, 240, 1200, 240, 1),
		},
		nil,
		1200,
		t,
	)
}

// now TTL of raw/first rollup is not enough but the two rollups are disabled, so must use raw
func TestAlignRequests2RollupsDisabled(t *testing.T) {
	testAlign([]models.Req{
		reqRaw("a", 0, 30, 800, 10, consolidation.Avg, 0, 0),
		reqRaw("b", 0, 30, 800, 60, consolidation.Avg, 1, 0),
	},
		[][]conf.Retention{
			{
				conf.NewRetentionMT(10, 1100, 0, 0, true), // just not long enough
				conf.NewRetentionMT(120, 1199, 600, 2, false),
				conf.NewRetentionMT(240, 1200, 600, 2, false),
			},
			{
				conf.NewRetentionMT(60, 1100, 0, 0, true), // just not long enough
				conf.NewRetentionMT(240, 1200, 600, 2, false),
			},
		},
		[]models.Req{
			reqOut("a", 0, 30, 800, 10, consolidation.Avg, 0, 0, 0, 10, 1100, 60, 6),
			reqOut("b", 0, 30, 800, 60, consolidation.Avg, 1, 0, 0, 60, 1100, 60, 1),
		},
		nil,
		1200,
		t,
	)
}
func TestAlignRequestsHuh(t *testing.T) {
	testAlign([]models.Req{
		reqRaw("a", 0, 30, 800, 10, consolidation.Avg, 0, 0),
		reqRaw("b", 0, 30, 800, 60, consolidation.Avg, 1, 0),
	},
		[][]conf.Retention{
			{
				conf.NewRetentionMT(1, 1000, 0, 0, true),
				conf.NewRetentionMT(120, 1080, 600, 2, true),
				conf.NewRetentionMT(240, 1200, 600, 2, false),
			},
			{
				conf.NewRetentionMT(60, 1100, 0, 0, true),
				conf.NewRetentionMT(240, 1200, 600, 2, false),
			},
		},
		[]models.Req{
			reqOut("a", 0, 30, 800, 10, consolidation.Avg, 0, 0, 1, 120, 1080, 120, 1),
			reqOut("b", 0, 30, 800, 60, consolidation.Avg, 1, 0, 0, 60, 1100, 120, 2),
		},
		nil,
		1200,
		t,
	)
}

var result []models.Req

func BenchmarkAlignRequests(b *testing.B) {
	var res []models.Req
	reqs := []models.Req{
		reqRaw("a", 0, 3600*24*7, 1000, 10, consolidation.Avg, 0, 0),
		reqRaw("b", 0, 3600*24*7, 1000, 30, consolidation.Avg, 1, 0),
		reqRaw("c", 0, 3600*24*7, 1000, 60, consolidation.Avg, 2, 0),
	}
	mdata.Schemas = conf.Schemas([]conf.Schema{
		{
			Pattern: regexp.MustCompile("a"),
			Retentions: conf.Retentions(
				[]conf.Retention{
					conf.NewRetentionMT(10, 35*24*3600, 0, 0, true),
					conf.NewRetentionMT(600, 60*24*3600, 0, 0, true),
					conf.NewRetentionMT(7200, 180*24*3600, 0, 0, true),
					conf.NewRetentionMT(21600, 2*365*24*3600, 0, 0, true),
				}),
		},
		{
			Pattern: regexp.MustCompile("b"),
			Retentions: conf.Retentions(
				[]conf.Retention{
					conf.NewRetentionMT(30, 35*24*3600, 0, 0, true),
					conf.NewRetentionMT(600, 60*24*3600, 0, 0, true),
					conf.NewRetentionMT(7200, 180*24*3600, 0, 0, true),
					conf.NewRetentionMT(21600, 2*365*24*3600, 0, 0, true),
				}),
		},
		{
			Pattern: regexp.MustCompile(".*"),
			Retentions: conf.Retentions(
				[]conf.Retention{
					conf.NewRetentionMT(60, 35*24*3600, 0, 0, true),
					conf.NewRetentionMT(600, 60*24*3600, 0, 0, true),
					conf.NewRetentionMT(7200, 180*24*3600, 0, 0, true),
					conf.NewRetentionMT(21600, 2*365*24*3600, 0, 0, true),
				}),
		},
	})

	for n := 0; n < b.N; n++ {
		res, _ = alignRequests(14*24*3600, reqs)
	}
	result = res
}
