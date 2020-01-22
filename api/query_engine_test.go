package api

import (
	"regexp"
	"sort"
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
// passing mpprSoft/mpprHard 0 means we will set them automatically such that they will never be hit
func testPlan(reqs []models.Req, retentions []conf.Retentions, outReqs []models.Req, outErr error, now uint32, mpprSoft, mpprHard int, t *testing.T) {
	var schemas []conf.Schema

	maxPointsPerReqSoft := mpprSoft

	for _, ret := range retentions {
		schemas = append(schemas, conf.Schema{
			Pattern:    regexp.MustCompile(".*"),
			Retentions: ret,
		})
		if mpprSoft == 0 {
			// make sure maxPointsPerReqSoft is high enough
			points := (int(reqs[0].To-reqs[0].From) / ret.Rets[0].SecondsPerPoint) * len(reqs)
			if points > maxPointsPerReqSoft {
				maxPointsPerReqSoft = points
			}
		}
	}
	maxPointsPerReqHard = mpprHard
	if mpprHard == 0 {
		maxPointsPerReqHard = maxPointsPerReqSoft * 10
	}

	// Note that conf.Schemas is "expanded" to create a new rule for each rollup
	// thus SchemasID must accommodate for this!
	mdata.Schemas = conf.NewSchemas(schemas)
	//spew.Dump(mdata.Schemas)
	out, err := planRequests(now, reqs[0].From, reqs[0].To, getReqMap(reqs), 0, maxPointsPerReqSoft, maxPointsPerReqHard)
	if err != outErr {
		t.Errorf("different err value expected: %v, got: %v", outErr, err)
	}
	if err == nil {
		if int(out.cnt) != len(outReqs) {
			t.Errorf("different number of requests expected: %v, got: %v", len(outReqs), out.cnt)
		} else {
			got := out.List()
			sort.Slice(got, func(i, j int) bool { return test.KeyToInt(got[i].MKey) < test.KeyToInt(got[j].MKey) })
			for r, exp := range outReqs {
				if !exp.Equals(got[r]) {
					t.Errorf("request %d:\nexpected: %v\n     got: %v", r, exp.DebugString(), got[r].DebugString())
				}
			}
		}
	}
}

// There are a lot of factors to consider. I haven't found a practical way to test all combinations of every factor
// but the approach taken in the functions below should be close enough.
// different test functions:
// * one or both may need to be pushed to rollup to meet TTL
// * use different native resolution within schemas (e.g. skip archives)
// within a test:
// * whether reqs use the exact same schema or different schemas that happen to be identical
// * zero, one or more PNGroups
// * soft limit breach
// * retention ready status
// * whether both need upping interval or not

func TestPlanRequests_SameInterval_SameTTL_RawOnly_RawMatches(t *testing.T) {
	in, out := generate(30, 60, []reqProp{
		NewReqProp(60, 0, 0),
		NewReqProp(60, 0, 0),
	})
	rets := []conf.Retentions{
		conf.MustParseRetentions("60s:1200s:60s:2:true"),
	}
	adjust(&out[0], 0, 60, 60, 1200)
	adjust(&out[1], 0, 60, 60, 1200)
	testPlan(in, rets, out, nil, 1200, 0, 0, t)

	// also test what happens when two series use distinct, but equal schemas
	rets = append(rets, rets[0])
	in[1].SchemaId, out[1].SchemaId = 1, 1
	testPlan(in, rets, out, nil, 1200, 0, 0, t)

	// also test what happens when one of them hasn't been ready long enough or is not ready at all
	for _, r := range []conf.Retentions{
		conf.MustParseRetentions("60s:1200s:60s:2:31"),
		conf.MustParseRetentions("60s:1200s:60s:2:false"),
	} {
		rets[0] = r
		//spew.Dump(rets)
		//spew.Dump(in)
		//spew.Dump(out)
		testPlan(in, rets, out, errUnSatisfiable, 1200, 0, 0, t)
	}
	// but to be clear, when it is ready, it is satisfiable
	for _, r := range []conf.Retentions{
		conf.MustParseRetentions("60s:1200s:60s:2:30"),
		conf.MustParseRetentions("60s:1200s:60s:2:29"),
		conf.MustParseRetentions("60s:1200s:60s:2:true"),
	} {
		rets[0] = r
		testPlan(in, rets, out, nil, 1200, 0, 0, t)
	}
}

/*
func copy2(a, b []models.Req) ([]models.Req, []models.Req) {
	a2 := make([]models.Req, len(a))
	b2 := make([]models.Req, len(b))
	copy(a2, a)
	copy(b2, b)
	return a2, b2
}
*/

func TestPlanRequests_DifferentInterval_SameTTL_RawOnly_RawMatches(t *testing.T) {
	in, out := generate(0, 30, []reqProp{
		NewReqProp(10, 0, 0),
		NewReqProp(60, 1, 0),
	})
	adjust(&out[0], 0, 10, 10, 1200)
	adjust(&out[1], 0, 60, 60, 1200)
	rets := []conf.Retentions{
		conf.MustParseRetentions("10s:1200s:60s:2:true"),
		conf.MustParseRetentions("60s:1200s:60s:2:true"),
	}
	t.Run("NoPNGroups", func(t *testing.T) {
		testPlan(in, rets, out, nil, 1200, 0, 0, t)
	})

	t.Run("DifferentPNGroups", func(t *testing.T) {
		// nothing should change
		in[0].PNGroup, out[0].PNGroup = 123, 123
		in[1].PNGroup, out[1].PNGroup = 124, 124
		testPlan(in, rets, out, nil, 1200, 0, 0, t)
	})
	t.Run("SamePNGroups", func(t *testing.T) {
		// should be normalized to the same interval
		in[0].PNGroup, out[0].PNGroup = 123, 123
		in[1].PNGroup, out[1].PNGroup = 123, 123
		adjust(&out[0], 0, 10, 60, 1200)
		testPlan(in, rets, out, nil, 1200, 0, 0, t)
	})
}

func TestPlanRequests_DifferentInterval_DifferentTTL_RawOnly_1RawShort(t *testing.T) {
	in, out := generate(0, 1000, []reqProp{
		NewReqProp(10, 0, 0),
		NewReqProp(60, 1, 0),
	})
	rets := []conf.Retentions{
		conf.MustParseRetentions("10s:800s:60s:2:true"),
		conf.MustParseRetentions("60s:1080s:60s:2:true"),
	}
	adjust(&out[0], 0, 10, 10, 800)
	adjust(&out[1], 0, 60, 60, 1080)
	t.Run("NoPNGroups", func(t *testing.T) {
		testPlan(in, rets, out, nil, 1200, 0, 0, t)
	})

	t.Run("DifferentPNGroups", func(t *testing.T) {
		// nothing should change
		in[0].PNGroup, out[0].PNGroup = 123, 123
		in[1].PNGroup, out[1].PNGroup = 124, 124
		testPlan(in, rets, out, nil, 1200, 0, 0, t)
	})
	t.Run("SamePNGroups", func(t *testing.T) {
		// should be normalized to the same interval
		in[0].PNGroup, out[0].PNGroup = 123, 123
		in[1].PNGroup, out[1].PNGroup = 123, 123
		adjust(&out[0], 0, 10, 60, 800)
		testPlan(in, rets, out, nil, 1200, 0, 0, t)
	})

}

func TestPlanRequests_DifferentInterval_DifferentTTL_1RawOnly1RawAndRollups_1Raw1Rollup(t *testing.T) {
	in, out := generate(0, 1000, []reqProp{
		NewReqProp(10, 0, 0),
		NewReqProp(60, 2, 0),
	})
	rets := []conf.Retentions{
		conf.MustParseRetentions("10s:1080s:60s:2:true,30s:1500s:60s:2:true"),
		conf.MustParseRetentions("60s:1320s:60s:2:true"),
	}
	adjust(&out[0], 1, 30, 30, 1500)
	adjust(&out[1], 0, 60, 60, 1320)
	t.Run("Base", func(t *testing.T) {
		testPlan(in, rets, out, nil, 1200, 0, 0, t)
	})

	t.Run("SameButTTLsNotLongEnough", func(t *testing.T) {
		rets = []conf.Retentions{
			conf.MustParseRetentions("10s:1080s:60s:2:true,30s:1140s:60s:2:true"),
			conf.MustParseRetentions("60s:1020s:60s:2:true"),
		}
		adjust(&out[0], 1, 30, 30, 1140)
		adjust(&out[1], 0, 60, 60, 1020)
		testPlan(in, rets, out, nil, 1200, 0, 0, t)
	})

	t.Run("ArchiveWeNeedIsNotReady", func(t *testing.T) {
		rets[0] = conf.MustParseRetentions("10s:1080s:60s:2:true,30s:1500s:60s:2:false")
		rets[1] = conf.MustParseRetentions("60s:1320s:60s:2:true")
		adjust(&out[0], 0, 10, 10, 1080)
		adjust(&out[1], 0, 60, 60, 1320)
		//spew.Dump(rets)
		testPlan(in, rets, out, nil, 1200, 0, 0, t)
	})

}

// like the above test, except the one that was already long enough has a rollup (that we don't use)
// and the short one only has raw.
func TestPlanRequests_DifferentInt_DifferentTTL_1RawOnly1RawAndRollups_1RawShort(t *testing.T) {
	in, out := generate(0, 1000, []reqProp{
		NewReqProp(10, 0, 0),
		NewReqProp(60, 1, 0),
	})
	rets := []conf.Retentions{
		conf.MustParseRetentions("10s:800s:60s:2:true"),
		conf.MustParseRetentions("60s:1200s:60s:2:true,5m:3000s:5min:2:true"), // extra rollup that we don't care for
	}
	adjust(&out[0], 0, 10, 10, 800)
	adjust(&out[1], 0, 60, 60, 1200)
	testPlan(in, rets, out, nil, 1200, 0, 0, t)

	t.Run("RawArchiveNotReady", func(t *testing.T) {
		// should switch to rollup
		rets[1] = conf.MustParseRetentions("60s:1200s:60s:2:false,5m:3000s:5min:2:true")
		adjust(&out[1], 1, 300, 300, 3000)
		testPlan(in, rets, out, nil, 3000, 0, 0, t)
	})
}

// 2 series with different raw intervals from the same schemas. Both requests should use the raw archive
func TestPlanRequestsMultiIntervalsUseRaw(t *testing.T) {
	in, out := generate(0, 1000, []reqProp{
		NewReqProp(10, 0, 0),
		NewReqProp(30, 1, 0),
	})
	rets := []conf.Retentions{
		conf.MustParseRetentions("10s:800s:60s:2:true,60s:1200s:5min:2:true"),
	}
	adjust(&out[0], 0, 10, 10, 800)
	adjust(&out[1], 0, 30, 30, 1200)
	testPlan(in, rets, out, nil, 800, 0, 0, t)
}

// 3 series with different raw intervals from the same schemas. TTL causes both to go to first rollup, which for one of them is raw
func TestPlanRequestsMultipleIntervalsPerSchema(t *testing.T) {
	in, out := generate(0, 1000, []reqProp{
		NewReqProp(1, 0, 0),
		NewReqProp(10, 1, 0),
		NewReqProp(60, 1, 0),
	})
	rets := []conf.Retentions{
		conf.MustParseRetentions("1s:800s:2h:2:true,60s:1140s:1h:2:true"),
	}
	adjust(&out[0], 1, 60, 60, 1140)
	adjust(&out[1], 0, 10, 10, 1140) // note how it has archive 10
	adjust(&out[2], 0, 60, 60, 1140)
	testPlan(in, rets, out, nil, 1200, t)
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
	if err != nil {
		return []models.Req{}, err
	}
	return out.List(), err
}

func TestGettingOneNextBiggerAgg(t *testing.T) {
	// we ask for 1 day worth, arch & out interval of 1s, ttl of 1h
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
		res, _ = planRequests(14*24*3600, 0, 3600*24*7, reqs, 0, 0, 0)
	}
	result = res
}
