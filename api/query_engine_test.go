package api

import (
	"math"
	"regexp"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/test"
)

func getReqMap(reqs []models.Req) *ReqMap {
	rm := NewReqMap()
	for _, r := range reqs {
		rm.Add(r)
	}
	return rm
}

// testPlan verifies the aligment of the given requests, given the retentions (one or more patterns, one or more retentions each)
func testPlan(reqs []models.Req, retentions []conf.Retentions, outReqs []models.Req, outErr error, now uint32, t *testing.T) {
	var schemas []conf.Schema
	oriMaxPointsPerReqSoft := maxPointsPerReqSoft
	oriMaxPointsPerHardReq := maxPointsPerReqHard

	for _, ret := range retentions {
		schemas = append(schemas, conf.Schema{
			Pattern:    regexp.MustCompile(".*"),
			Retentions: ret,
		})
		// make sure maxPointsPerReqSoft is high enough
		points := (int(reqs[0].To-reqs[0].From) / ret.Rets[0].SecondsPerPoint) * len(reqs)
		if points > maxPointsPerReqSoft {
			maxPointsPerReqSoft = points
		}
	}
	maxPointsPerReqHard = maxPointsPerReqSoft * 10

	mdata.Schemas = conf.NewSchemas(schemas)
	out, err := planRequests(now, reqs[0].From, reqs[0].To, getReqMap(reqs), 0)
	if err != outErr {
		t.Errorf("different err value expected: %v, got: %v", outErr, err)
	}
	if int(out.cnt) != len(outReqs) {
		t.Errorf("different number of requests expected: %v, got: %v", len(outReqs), out.cnt)
	} else {
		got := out.List()
		for r, exp := range outReqs {
			if !exp.Equals(got[r]) {
				t.Errorf("request %d:\nexpected: %v\n     got: %v", r, exp.DebugString(), got[r].DebugString())
			}
		}
	}

	maxPointsPerReqSoft = oriMaxPointsPerReqSoft
	maxPointsPerReqHard = oriMaxPointsPerHardReq
}

// 2 series with equal schema of 1 raw archive. tsRange within TTL of raw.return the raw data
func TestPlanRequestsBasic(t *testing.T) {
	testPlan([]models.Req{
		reqRaw(test.GetMKey(1), 0, 30, 0, 60, consolidation.Avg, 0, 0),
		reqRaw(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 0, 0),
	},
		[]conf.Retentions{
			conf.BuildFromRetentions(
				conf.NewRetentionMT(60, 1200, 0, 0, 0),
			),
		},
		[]models.Req{
			reqOut(test.GetMKey(1), 0, 30, 0, 60, consolidation.Avg, 0, 0, 0, 60, 1200, 60, 1),
			reqOut(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 0, 0, 0, 60, 1200, 60, 1),
		},
		nil,
		1200,
		t,
	)
}

// 2 series with distinct but equal schemas of 1 raw archive. tsRange within TTL for both. return the raw data.
func TestPlanRequestsBasicDiff(t *testing.T) {
	testPlan([]models.Req{
		reqRaw(test.GetMKey(1), 0, 30, 0, 60, consolidation.Avg, 0, 0),
		reqRaw(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 1, 0),
	},
		[]conf.Retentions{
			conf.BuildFromRetentions(
				conf.NewRetentionMT(60, 1200, 0, 0, 0),
			),
			conf.BuildFromRetentions(
				conf.NewRetentionMT(60, 1200, 0, 0, 0),
			),
		},
		[]models.Req{
			reqOut(test.GetMKey(1), 0, 30, 0, 60, consolidation.Avg, 0, 0, 0, 60, 1200, 60, 1),
			reqOut(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 1, 0, 0, 60, 1200, 60, 1),
		},
		nil,
		1200,
		t,
	)
}

// 2 series with distinct schemas of different raw archive. tsRange within TTL for both. return them at their native intervals
func TestPlanRequestsDifferentIntervals(t *testing.T) {
	testPlan([]models.Req{
		reqRaw(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0),
		reqRaw(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 1, 0),
	},
		[]conf.Retentions{
			conf.BuildFromRetentions(
				conf.NewRetentionMT(10, 1200, 0, 0, 0),
			),
			conf.BuildFromRetentions(
				conf.NewRetentionMT(60, 1200, 0, 0, 0),
			),
		},
		[]models.Req{
			reqOut(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0, 0, 10, 1200, 10, 1),
			reqOut(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 1, 0, 0, 60, 1200, 60, 1),
		},
		nil,
		1200,
		t,
	)
}

// 2 series with distinct schemas of 1 raw archive with different interval and TTL . tsRange within TTL for only one of them. return them at their native intervals, because there is no rollup
func TestPlanRequestsBasicBestEffort(t *testing.T) {
	testPlan([]models.Req{
		reqRaw(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0),
		reqRaw(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 1, 0),
	},
		[]conf.Retentions{
			conf.BuildFromRetentions(
				conf.NewRetentionMT(10, 800, 0, 0, 0),
			),
			conf.BuildFromRetentions(
				conf.NewRetentionMT(60, 1100, 0, 0, 0),
			),
		},
		[]models.Req{
			reqOut(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0, 0, 10, 800, 10, 1),
			reqOut(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 1, 0, 0, 60, 1100, 60, 1),
		},
		nil,
		1200,
		t,
	)
}

// 2 series with different raw intervals from the same schemas. Both requests should use raw
func TestPlanRequestsMultiIntervalsWithRuntimeConsolidation(t *testing.T) {
	testPlan([]models.Req{
		reqRaw(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0),
		reqRaw(test.GetMKey(2), 0, 30, 0, 30, consolidation.Avg, 0, 0),
	},
		[]conf.Retentions{
			conf.BuildFromRetentions(
				conf.NewRetentionMT(10, 800, 0, 0, 0),
				conf.NewRetentionMT(60, 1200, 0, 0, 0),
			),
		},
		[]models.Req{
			reqOut(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0, 0, 10, 800, 10, 1),
			reqOut(test.GetMKey(2), 0, 30, 0, 30, consolidation.Avg, 0, 0, 0, 30, 800, 30, 1),
		},
		nil,
		800,
		t,
	)
}

// 2 series with different raw intervals from the same schemas. TTL causes both to go to first rollup
func TestPlanRequestsMultipleIntervalsPerSchema(t *testing.T) {
	testPlan([]models.Req{
		reqRaw(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0),
		reqRaw(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 1, 0),
	},
		[]conf.Retentions{
			conf.BuildFromRetentions(
				conf.NewRetentionMT(1, 800, 0, 0, 0),
				conf.NewRetentionMT(60, 1100, 0, 0, 0),
			),
		},
		[]models.Req{
			reqOut(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0, 1, 60, 800, 60, 1),
			reqOut(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 1, 0, 0, 60, 1100, 60, 1),
		},
		nil,
		1200,
		t,
	)
}

// 2 series requested with different raw intervals from different schemas. req 0-30. now 1200. one has short raw. other has short raw + good rollup
func TestPlanRequestsHalfGood(t *testing.T) {
	testPlan([]models.Req{
		reqRaw(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0),
		reqRaw(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 1, 0),
	},
		[]conf.Retentions{
			conf.BuildFromRetentions(
				conf.NewRetentionMT(10, 800, 0, 0, 0),
			),
			conf.BuildFromRetentions(
				conf.NewRetentionMT(60, 1100, 0, 0, 0),
				conf.NewRetentionMT(120, 1200, 0, 0, 0),
			),
		},
		[]models.Req{
			reqOut(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0, 0, 10, 800, 10, 1),
			reqOut(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 1, 0, 1, 120, 1200, 120, 1),
		},
		nil,
		1200,
		t,
	)
}

// 2 series requested with different raw intervals from different schemas. req 0-30. now 1200. both have short raw + good rollup
func TestPlanRequestsGoodRollup(t *testing.T) {
	testPlan([]models.Req{
		reqRaw(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0),
		reqRaw(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 2, 0),
	},
		[]conf.Retentions{
			conf.BuildFromRetentions(
				conf.NewRetentionMT(10, 1199, 0, 0, 0), // just not long enough
				conf.NewRetentionMT(120, 1200, 600, 2, 0),
			),
			conf.BuildFromRetentions(
				conf.NewRetentionMT(60, 1199, 0, 0, 0), // just not long enough
				conf.NewRetentionMT(120, 1200, 600, 2, 0),
			),
		},
		[]models.Req{
			reqOut(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0, 1, 120, 1200, 120, 1),
			reqOut(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 2, 0, 1, 120, 1200, 120, 1),
		},
		nil,
		1200,
		t,
	)
}

// 2 series requested with different raw intervals, and rollup intervals from different schemas. req 0-30. now 1200. both have short raw + good rollup
func TestPlanRequestsDiffGoodRollup(t *testing.T) {
	testPlan([]models.Req{
		reqRaw(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0),
		reqRaw(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 2, 0),
	},
		[]conf.Retentions{
			conf.BuildFromRetentions(
				conf.NewRetentionMT(10, 1199, 0, 0, 0), // just not long enough
				conf.NewRetentionMT(100, 1200, 600, 2, 0),
			),
			conf.BuildFromRetentions(
				conf.NewRetentionMT(60, 1199, 0, 0, 0), // just not long enough
				conf.NewRetentionMT(600, 1200, 600, 2, 0),
			),
		},
		[]models.Req{
			reqOut(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0, 1, 100, 1200, 600, 6),
			reqOut(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 2, 0, 1, 600, 1200, 600, 1),
		},
		nil,
		1200,
		t,
	)
}

// now raw is short and we have a rollup we can use instead, at same interval as one of the raws
func TestPlanRequestsWeird(t *testing.T) {
	testPlan([]models.Req{
		reqRaw(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0),
		reqRaw(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 2, 0),
	},
		[]conf.Retentions{
			conf.BuildFromRetentions(
				conf.NewRetentionMT(10, 1199, 0, 0, 0),
				conf.NewRetentionMT(60, 1200, 600, 2, 0),
			),
			conf.BuildFromRetentions(
				conf.NewRetentionMT(60, 1200, 0, 0, 0),
			),
		},
		[]models.Req{
			reqOut(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0, 1, 60, 1200, 60, 1),
			reqOut(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 2, 0, 0, 60, 1200, 60, 1),
		},
		nil,
		1200,
		t,
	)
}

// now TTL of first rollup is *just* enough
func TestPlanRequestsWeird2(t *testing.T) {
	testPlan([]models.Req{
		reqRaw(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0),
		reqRaw(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 2, 0),
	},
		[]conf.Retentions{
			conf.BuildFromRetentions(
				conf.NewRetentionMT(10, 1100, 0, 0, 0), // just not long enough
				conf.NewRetentionMT(120, 1200, 600, 2, 0),
			),
			conf.BuildFromRetentions(
				conf.NewRetentionMT(60, 1100, 0, 0, 0), // just not long enough
				conf.NewRetentionMT(120, 1200, 600, 2, 0),
			),
		},
		[]models.Req{
			reqOut(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0, 1, 120, 1200, 120, 1),
			reqOut(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 2, 0, 1, 120, 1200, 120, 1),
		},
		nil,
		1200,
		t,
	)
}

// now TTL of first rollup is not enough but we have no other choice but to use it
func TestPlanRequestsNoOtherChoice(t *testing.T) {
	testPlan([]models.Req{
		reqRaw(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0),
		reqRaw(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 2, 0),
	},
		[]conf.Retentions{
			conf.BuildFromRetentions(
				conf.NewRetentionMT(10, 1100, 0, 0, 0),
				conf.NewRetentionMT(120, 1199, 600, 2, 0),
			),
			conf.BuildFromRetentions(
				conf.NewRetentionMT(60, 1100, 0, 0, 0),
				conf.NewRetentionMT(120, 1199, 600, 2, 0),
			),
		},
		[]models.Req{
			reqOut(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0, 1, 120, 1199, 120, 1),
			reqOut(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 2, 0, 1, 120, 1199, 120, 1),
		},
		nil,
		1200,
		t,
	)
}

// now TTL of first rollup is not enough and we have a 3rd band to use
func TestPlanRequests3rdBand(t *testing.T) {
	testPlan([]models.Req{
		reqRaw(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0),
		reqRaw(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 3, 0),
	},
		[]conf.Retentions{
			conf.BuildFromRetentions(
				conf.NewRetentionMT(1, 1100, 0, 0, 0),
				conf.NewRetentionMT(120, 1199, 600, 2, 0),
				conf.NewRetentionMT(240, 1200, 600, 2, 0),
			),
			conf.BuildFromRetentions(
				conf.NewRetentionMT(60, 1100, 0, 0, 0),
				conf.NewRetentionMT(240, 1200, 600, 2, 0),
			),
		},
		[]models.Req{
			reqOut(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0, 2, 240, 1200, 240, 1),
			reqOut(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 3, 0, 1, 240, 1200, 240, 1),
		},
		nil,
		1200,
		t,
	)
}

// now TTL of raw/first rollup is not enough but the two rollups are disabled, so must use raw
func TestPlanRequests2RollupsDisabled(t *testing.T) {
	testPlan([]models.Req{
		reqRaw(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0),
		reqRaw(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 3, 0),
	},
		[]conf.Retentions{
			conf.BuildFromRetentions(
				conf.NewRetentionMT(10, 1100, 0, 0, 0), // just not long enough
				conf.NewRetentionMT(120, 1199, 600, 2, math.MaxUint32),
				conf.NewRetentionMT(240, 1200, 600, 2, math.MaxUint32),
			),
			conf.BuildFromRetentions(
				conf.NewRetentionMT(60, 1100, 0, 0, 0), // just not long enough
				conf.NewRetentionMT(240, 1200, 600, 2, math.MaxUint32),
			),
		},
		[]models.Req{
			reqOut(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0, 0, 10, 1100, 60, 6),
			reqOut(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 3, 0, 0, 60, 1100, 60, 1),
		},
		nil,
		1200,
		t,
	)
}
func TestPlanRequestsHuh(t *testing.T) {
	testPlan([]models.Req{
		reqRaw(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0),
		reqRaw(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 3, 0),
	},
		[]conf.Retentions{
			conf.BuildFromRetentions(
				conf.NewRetentionMT(1, 1000, 0, 0, 0),
				conf.NewRetentionMT(120, 1080, 600, 2, 0),
				conf.NewRetentionMT(240, 1200, 600, 2, math.MaxUint32),
			),
			conf.BuildFromRetentions(
				conf.NewRetentionMT(60, 1100, 0, 0, 0),
				conf.NewRetentionMT(240, 1200, 600, 2, math.MaxUint32),
			),
		},
		[]models.Req{
			reqOut(test.GetMKey(1), 0, 30, 0, 10, consolidation.Avg, 0, 0, 1, 120, 1080, 120, 1),
			reqOut(test.GetMKey(2), 0, 30, 0, 60, consolidation.Avg, 3, 0, 0, 60, 1100, 120, 2),
		},
		nil,
		1200,
		t,
	)
}

func TestPlanRequestsDifferentReadyStates(t *testing.T) {
	testPlan([]models.Req{
		reqRaw(test.GetMKey(1), 100, 300, 0, 1, consolidation.Avg, 0, 0),
	},
		[]conf.Retentions{
			conf.BuildFromRetentions(
				conf.NewRetentionMT(1, 300, 120, 5, 0),              // TTL not good enough
				conf.NewRetentionMT(5, 450, 600, 4, math.MaxUint32), // TTL good, but not ready
				conf.NewRetentionMT(10, 460, 600, 3, 150),           // TTL good, but not ready since long enough
				conf.NewRetentionMT(20, 470, 600, 2, 101),           // TTL good, but not ready since long enough
				conf.NewRetentionMT(60, 480, 600, 1, 100),           // TTL good and ready since long enough
			),
		},
		[]models.Req{
			reqOut(test.GetMKey(1), 100, 300, 0, 1, consolidation.Avg, 0, 0, 4, 60, 480, 60, 1),
		},
		nil,
		500,
		t,
	)
}

var hour uint32 = 60 * 60
var day uint32 = 24 * hour

func testMaxPointsPerReq(maxPointsSoft, maxPointsHard int, reqs []models.Req, t *testing.T) ([]models.Req, error) {
	origMaxPointsPerReqSoft := maxPointsPerReqSoft
	origMaxPointsPerReqHard := maxPointsPerReqHard
	maxPointsPerReqSoft = maxPointsSoft
	maxPointsPerReqHard = maxPointsHard

	mdata.Schemas = conf.NewSchemas([]conf.Schema{{
		Pattern: regexp.MustCompile(".*"),
		Retentions: conf.BuildFromRetentions(
			conf.NewRetentionMT(1, 2*day, 600, 2, 0),
			conf.NewRetentionMT(60, 7*day, 600, 2, 0),
			conf.NewRetentionMT(3600, 30*day, 600, 2, 0),
		),
	}})

	out, err := planRequests(30*day, reqs[0].From, reqs[0].To, getReqMap(reqs), 0)
	maxPointsPerReqSoft = origMaxPointsPerReqSoft
	maxPointsPerReqHard = origMaxPointsPerReqHard
	return out.List(), err
}

func TestGettingOneNextBiggerAgg(t *testing.T) {
	reqs := []models.Req{
		reqOut(test.GetMKey(1), 29*day, 30*day, 0, 1, consolidation.Avg, 0, 0, 0, 1, hour, 1, 1),
	}

	// without maxPointsPerReqSoft = 23*hour we'd get archive 0 for this request,
	// with it we expect the aggregation to get bumped to the next one
	out, err := testMaxPointsPerReq(int(23*hour), 0, reqs, t)
	if err != nil {
		t.Fatalf("expected to get no error")
	}
	if out[0].Archive != 1 {
		t.Errorf("expected archive %d, but got archive %d", 1, out[0].Archive)
	}
}

func TestGettingTwoNextBiggerAgg(t *testing.T) {
	reqs := []models.Req{
		reqOut(test.GetMKey(1), 29*day, 30*day, 0, 1, consolidation.Avg, 0, 0, 0, 1, hour, 1, 1),
	}

	// maxPointsPerReqSoft only allows 24 points, so the aggregation 2 with
	// 3600 SecondsPerPoint should be chosen for our request of 1 day
	out, err := testMaxPointsPerReq(24, 0, reqs, t)
	if err != nil {
		t.Fatalf("expected to get no error")
	}
	if out[0].Archive != 2 {
		t.Errorf("expected archive %d, but got archive %d", 2, out[0].Archive)
	}
}

func TestMaxPointsPerReqHardLimit(t *testing.T) {
	reqs := []models.Req{
		reqOut(test.GetMKey(1), 29*day, 30*day, 0, 1, consolidation.Avg, 0, 0, 0, 1, hour, 1, 1),
	}
	// we're requesting one day and the lowest resolution aggregation has 3600 seconds per point,
	// so there should be an error because we only allow max 23 points per request
	_, err := testMaxPointsPerReq(22, 23, reqs, t)
	if err != errMaxPointsPerReq {
		t.Fatalf("expected to get an error")
	}
}

var result *ReqsPlan

func BenchmarkPlanRequestsSamePNGroup(b *testing.B) {
	var res *ReqsPlan
	reqs := NewReqMap()
	reqs.Add(reqRaw(test.GetMKey(1), 0, 3600*24*7, 0, 10, consolidation.Avg, 0, 0))
	reqs.Add(reqRaw(test.GetMKey(2), 0, 3600*24*7, 0, 30, consolidation.Avg, 4, 0))
	reqs.Add(reqRaw(test.GetMKey(3), 0, 3600*24*7, 0, 60, consolidation.Avg, 8, 0))
	mdata.Schemas = conf.NewSchemas([]conf.Schema{
		{
			Pattern: regexp.MustCompile("a"),
			Retentions: conf.BuildFromRetentions(
				conf.NewRetentionMT(10, 35*24*3600, 0, 0, 0),
				conf.NewRetentionMT(600, 60*24*3600, 0, 0, 0),
				conf.NewRetentionMT(7200, 180*24*3600, 0, 0, 0),
				conf.NewRetentionMT(21600, 2*365*24*3600, 0, 0, 0),
			),
		},
		{
			Pattern: regexp.MustCompile("b"),
			Retentions: conf.BuildFromRetentions(
				conf.NewRetentionMT(30, 35*24*3600, 0, 0, 0),
				conf.NewRetentionMT(600, 60*24*3600, 0, 0, 0),
				conf.NewRetentionMT(7200, 180*24*3600, 0, 0, 0),
				conf.NewRetentionMT(21600, 2*365*24*3600, 0, 0, 0),
			),
		},
		{
			Pattern: regexp.MustCompile(".*"),
			Retentions: conf.BuildFromRetentions(
				conf.NewRetentionMT(60, 35*24*3600, 0, 0, 0),
				conf.NewRetentionMT(600, 60*24*3600, 0, 0, 0),
				conf.NewRetentionMT(7200, 180*24*3600, 0, 0, 0),
				conf.NewRetentionMT(21600, 2*365*24*3600, 0, 0, 0),
			),
		},
	})

	for n := 0; n < b.N; n++ {
		res, _ = planRequests(14*24*3600, 0, 3600*24*7, reqs, 0)
	}
	result = res
}
