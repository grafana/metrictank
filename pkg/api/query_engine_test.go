package api

import (
	"regexp"
	"sort"
	"testing"

	"github.com/grafana/metrictank/internal/consolidation"
	"github.com/grafana/metrictank/internal/mdata"
	"github.com/grafana/metrictank/internal/test"
	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/grafana/metrictank/pkg/conf"
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
func testPlan(reqs []models.Req, retentions []conf.Retentions, outReqs []models.Req, outErr error, now uint32, mpprSoft, mpprHard int, t *testing.T) *ReqsPlan {
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
	out, err := planRequests(now, getReqMap(reqs), 0, maxPointsPerReqSoft, maxPointsPerReqHard)
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
	return out
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

	// let's look at some MaxPointsPerReqSoft scenarios:
	// points fetched is for each request (to-from) / archInterval
	// so normally, 1000/10 + 1000 / 30 = ~133
	t.Run("WithMaxPointsPerReqSoftVeryTight", func(t *testing.T) {
		// this should still work as before, but just make the limit
		testPlan(in, rets, out, nil, 800, 134, 0, t)
	})
	t.Run("WithMaxPointsPerReqSoftBreached", func(t *testing.T) {
		// we breach so, one request at a time, it'll lower the resolution, if an interval is available...
		adjust(&out[0], 1, 60, 60, 1200)
		adjust(&out[1], 0, 30, 30, 1200)
		testPlan(in, rets, out, nil, 800, 130, 0, t)
		t.Run("WithHardVeryTight", func(t *testing.T) {
			// 1000/60 + 1000/30 =~ 46
			adjust(&out[0], 1, 60, 60, 1200)
			adjust(&out[1], 0, 30, 30, 1200)
			testPlan(in, rets, out, nil, 800, 130, 50, t)
		})
		t.Run("WithHardBreached", func(t *testing.T) {
			// 1000/60 + 1000/30 =~ 46
			adjust(&out[0], 1, 60, 60, 1200)
			adjust(&out[1], 0, 30, 30, 1200)
			testPlan(in, rets, out, errMaxPointsPerReq, 800, 130, 40, t)
		})
	})
}

// 2 identical singles, one requesting older data (> raw TTL).
// They should be independently planned
func TestPlanRequests_Singles_DifferentTimeRanges(t *testing.T) {
	// req0 400-800
	// req1 0-400 // with a now at 1200, archive 0 doesn't have long enough retention
	in, out := generate(400, 800, []reqProp{
		NewReqProp(10, 0, 0),
		NewReqProp(10, 0, 0),
	})
	rets := []conf.Retentions{
		conf.MustParseRetentions("10s:800s:60s:2:true,60s:1200s:5min:2:true"),
	}
	adjust(&out[0], 0, 10, 10, 800)
	adjust(&out[1], 1, 60, 60, 1200)
	in[1].From, out[1].From = 0, 0
	in[1].To, out[1].To = 400, 400
	testPlan(in, rets, out, nil, 1200, 0, 0, t)

	// If soft is slightly breached, only the high res data should be reduced
	// (800-400)/10 + (400-0)/60 = 46 points
	t.Run("WithMaxPointsPerReqSoftJustBreached", func(t *testing.T) {
		adjust(&out[0], 1, 60, 60, 1200)
		testPlan(in, rets, out, nil, 1200, 45, 0, t)
	})
}

// TestPlanRequestsMaxPointsPerReqSoft tests how maxPointsPerReqSoft gets applied.
// we validate that:
//   - requests are coarsened, PNGroup by PNGroup (we cannot predict PNGroup map iteration order, so we only test with 1 PNGroup),
//     and singles in groups by retention (in schemaID order)
//   - PNGroups obviously will need a common interval, which gets interesting when using multiple schemas
//   - coarsening continues until all data is fetched at its coarsest. At that point we may breach soft, but never hard
func TestPlanRequestsMaxPointsPerReqSoft(t *testing.T) {
	in, out := generate(0, 1000, []reqProp{
		// 4 singles from 2 different retentions
		NewReqProp(10, 0, 0),
		NewReqProp(10, 0, 0),
		NewReqProp(15, 3, 0),
		NewReqProp(15, 3, 0),
		// multi comprised of 2 reqs from different retentions (rather than the same retention, just to make it more interesting)
		NewReqProp(10, 0, 0),
		NewReqProp(15, 3, 0),
	})

	in[4].PNGroup = 123
	in[5].PNGroup = 123
	out[4].PNGroup = 123
	out[5].PNGroup = 123

	rets := []conf.Retentions{
		conf.MustParseRetentions("10s:1h,60s:2h,300s:7d"),
		conf.MustParseRetentions("15s:1h,30s:2h,120s:7d"),
	}

	// if we don't hit limits, everything will use archive 0, except
	// since the PNGroup has LCM(10,15) = 30, for one of them it'll read archive=1, for the archive it'll use archive 0 and consolidate by 3x
	adjust(&out[0], 0, 10, 10, 3600)
	adjust(&out[1], 0, 10, 10, 3600)
	adjust(&out[2], 0, 15, 15, 3600)
	adjust(&out[3], 0, 15, 15, 3600)

	adjust(&out[4], 0, 10, 30, 3600)
	adjust(&out[5], 1, 30, 30, 3600*2)

	plan := testPlan(in, rets, out, nil, 800, 0, 0, t)
	expPointsFetch := 1000/10 + 1000/10 + 1000/15 + 1000/15 + 1000/10 + 1000/30 // 465
	if int(plan.PointsFetch()) != expPointsFetch {
		t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
	}

	t.Run("WithMaxPointsPerReqSoftVeryTight466", func(t *testing.T) {
		// this should still work as before, but just make the limit
		plan := testPlan(in, rets, out, nil, 800, 466, 0, t)
		if int(plan.PointsFetch()) != expPointsFetch {
			t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
		}
	})

	// now we test a few cases where Soft is just aggressive enough that we need to do our first round of reductions
	t.Run("WithMaxPointsPerReqSoftBroken1", func(t *testing.T) {

		// the PNGroup will move to the 60s resolution
		adjust(&out[4], 1, 60, 60, 3600*2)
		adjust(&out[5], 1, 30, 60, 3600*2)
		expPointsFetch := 1000/10 + 1000/10 + 1000/15 + 1000/15 + 1000/60 + 1000/30 // 381

		t.Run("Soft464", func(t *testing.T) {
			plan := testPlan(in, rets, out, nil, 800, 464, 0, t)
			if int(plan.PointsFetch()) != expPointsFetch {
				t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
			}
		})
		t.Run("Soft382", func(t *testing.T) {
			plan := testPlan(in, rets, out, nil, 800, 382, 0, t)
			if int(plan.PointsFetch()) != expPointsFetch {
				t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
			}
		})
		t.Run("Soft381", func(t *testing.T) {
			plan := testPlan(in, rets, out, nil, 800, 381, 0, t)
			if int(plan.PointsFetch()) != expPointsFetch {
				t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
			}
		})
	})

	// getting tighter. we need 2 rounds of reductions
	t.Run("WithMaxPointsPerReqSoftBroken2", func(t *testing.T) {

		// the PNGroup will move to the 60s resolution, like before...
		adjust(&out[4], 1, 60, 60, 3600*2)
		adjust(&out[5], 1, 30, 60, 3600*2)
		// ... but now also the singles that have schemaID 0 will move to the next rollup
		adjust(&out[0], 1, 60, 60, 3600*2)
		adjust(&out[1], 1, 60, 60, 3600*2)
		expPointsFetch := 1000/60 + 1000/60 + 1000/15 + 1000/15 + 1000/60 + 1000/30 // 213

		t.Run("Soft380", func(t *testing.T) {
			plan := testPlan(in, rets, out, nil, 800, 380, 0, t)
			if int(plan.PointsFetch()) != expPointsFetch {
				t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
			}
		})
		t.Run("Soft213", func(t *testing.T) {
			plan := testPlan(in, rets, out, nil, 800, 213, 0, t)
			if int(plan.PointsFetch()) != expPointsFetch {
				t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
			}
		})
	})

	// getting tighter. we need 3 rounds of reductions
	t.Run("WithMaxPointsPerReqSoftBroken3", func(t *testing.T) {

		// the PNGroup will move to the 60s resolution, like before...
		adjust(&out[4], 1, 60, 60, 3600*2)
		adjust(&out[5], 1, 30, 60, 3600*2)
		// ... but now also the singles that have schemaID 0 will move to the next rollup like before ...
		adjust(&out[0], 1, 60, 60, 3600*2)
		adjust(&out[1], 1, 60, 60, 3600*2)
		// ... but now schemaID 1 based requests also move to the next rollup
		adjust(&out[2], 1, 30, 30, 3600*2)
		adjust(&out[3], 1, 30, 30, 3600*2)
		expPointsFetch := 1000/60 + 1000/60 + 1000/30 + 1000/30 + 1000/60 + 1000/30 // 147

		t.Run("Soft212", func(t *testing.T) {
			plan := testPlan(in, rets, out, nil, 800, 212, 0, t)
			if int(plan.PointsFetch()) != expPointsFetch {
				t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
			}
		})
		t.Run("Soft147", func(t *testing.T) {
			plan := testPlan(in, rets, out, nil, 800, 147, 0, t)
			if int(plan.PointsFetch()) != expPointsFetch {
				t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
			}
		})
	})

	// every request has been coarsened. but we need to accommodate an even tighter Soft limit
	// at this point we start over again and reduce the PNGroup even further
	t.Run("WithMaxPointsPerReqSoftBroken4", func(t *testing.T) {

		// singles are the same as before
		adjust(&out[0], 1, 60, 60, 3600*2)
		adjust(&out[1], 1, 60, 60, 3600*2)
		adjust(&out[2], 1, 30, 30, 3600*2)
		adjust(&out[3], 1, 30, 30, 3600*2)

		// but the PNGroup will be further reduced. from outInterval 60 to the next available one, which is 120
		// this moves the 2nd req to the next archive, but the first one can stay where it is!
		adjust(&out[4], 1, 60, 120, 3600*2)
		adjust(&out[5], 2, 120, 120, 3600*24*7)

		expPointsFetch := 1000/60 + 1000/60 + 1000/30 + 1000/30 + 1000/60 + 1000/120 // 122

		t.Run("Soft146", func(t *testing.T) {
			plan := testPlan(in, rets, out, nil, 800, 146, 0, t)
			if int(plan.PointsFetch()) != expPointsFetch {
				t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
			}
		})
		t.Run("Soft122", func(t *testing.T) {
			plan := testPlan(in, rets, out, nil, 800, 122, 0, t)
			if int(plan.PointsFetch()) != expPointsFetch {
				t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
			}
		})
	})

	// pushing even more, we now also reduce singles with schemaID 0 further
	t.Run("WithMaxPointsPerReqSoftBroken5", func(t *testing.T) {

		adjust(&out[0], 2, 300, 300, 3600*24*7)
		adjust(&out[1], 2, 300, 300, 3600*24*7)
		adjust(&out[2], 1, 30, 30, 3600*2)
		adjust(&out[3], 1, 30, 30, 3600*2)
		adjust(&out[4], 1, 60, 120, 3600*2)
		adjust(&out[5], 2, 120, 120, 3600*24*7)

		expPointsFetch := 1000/300 + 1000/300 + 1000/30 + 1000/30 + 1000/60 + 1000/120 // 96

		t.Run("Soft121", func(t *testing.T) {
			plan := testPlan(in, rets, out, nil, 800, 121, 0, t)
			if int(plan.PointsFetch()) != expPointsFetch {
				t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
			}
		})
		t.Run("Soft96", func(t *testing.T) {
			plan := testPlan(in, rets, out, nil, 800, 96, 0, t)
			if int(plan.PointsFetch()) != expPointsFetch {
				t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
			}
		})
	})
	// pushing even more, we now also reduce singles with schemaID 1 further
	t.Run("WithMaxPointsPerReqSoftBroken6", func(t *testing.T) {

		adjust(&out[0], 2, 300, 300, 3600*24*7)
		adjust(&out[1], 2, 300, 300, 3600*24*7)
		adjust(&out[2], 2, 120, 120, 3600*24*7)
		adjust(&out[3], 2, 120, 120, 3600*24*7)
		adjust(&out[4], 1, 60, 120, 3600*2)
		adjust(&out[5], 2, 120, 120, 3600*24*7)

		expPointsFetch := 1000/300 + 1000/300 + 1000/120 + 1000/120 + 1000/60 + 1000/120 // 46

		t.Run("Soft95", func(t *testing.T) {
			plan := testPlan(in, rets, out, nil, 800, 95, 0, t)
			if int(plan.PointsFetch()) != expPointsFetch {
				t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
			}
		})
		t.Run("Soft46", func(t *testing.T) {
			plan := testPlan(in, rets, out, nil, 800, 46, 0, t)
			if int(plan.PointsFetch()) != expPointsFetch {
				t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
			}
		})
	})

	// pushing even more, we circle back around and reduce the PNGroup further
	// from interval 120 whichever is the next higher one: 600
	t.Run("WithMaxPointsPerReqSoftBroken6", func(t *testing.T) {

		adjust(&out[0], 2, 300, 300, 3600*24*7)
		adjust(&out[1], 2, 300, 300, 3600*24*7)
		adjust(&out[2], 2, 120, 120, 3600*24*7)
		adjust(&out[3], 2, 120, 120, 3600*24*7)
		adjust(&out[4], 2, 300, 600, 3600*24*7)
		adjust(&out[5], 2, 120, 600, 3600*24*7)

		expPointsFetch := 1000/300 + 1000/300 + 1000/120 + 1000/120 + 1000/300 + 1000/120 // 33

		t.Run("Soft45", func(t *testing.T) {
			plan := testPlan(in, rets, out, nil, 800, 45, 0, t)
			if int(plan.PointsFetch()) != expPointsFetch {
				t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
			}
		})
		t.Run("Soft33", func(t *testing.T) {
			plan := testPlan(in, rets, out, nil, 800, 33, 0, t)
			if int(plan.PointsFetch()) != expPointsFetch {
				t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
			}
		})

		// now, the moment we've all been waiting for... since everything is reduced to the max, what happens if we try to go further?
		// we simply start breaching the soft limit.
		t.Run("Soft32", func(t *testing.T) {
			plan := testPlan(in, rets, out, nil, 800, 32, 0, t)
			if int(plan.PointsFetch()) != expPointsFetch {
				t.Errorf("points fetched expected %d, got %d", expPointsFetch, plan.PointsFetch())
			}

			// but we can get an error if we update the Hard limit instead:
			testPlan(in, rets, out, nil, 800, 32, 33, t)
			testPlan(in, rets, out, errMaxPointsPerReq, 800, 32, 32, t)
			testPlan(in, rets, out, errMaxPointsPerReq, 800, 32, 30, t)
			testPlan(in, rets, out, errMaxPointsPerReq, 800, 32, 1, t)
		})
	})
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
	testPlan(in, rets, out, nil, 1200, 0, 0, t)
}

// TestPlanRequests_NonRetInterval test what happens when series have a RawInterval that is != rets[0].Interval
func TestPlanRequests_NonRetInterval(t *testing.T) {
	in, out := generate(30, 60, []reqProp{
		NewReqProp(10, 0, 0),
		NewReqProp(15, 0, 0),
		NewReqProp(25, 0, 0),
	})
	rets := []conf.Retentions{
		conf.MustParseRetentions("1s:800s:2h:2:true,60s:1140s:1h:2:true"),
	}

	// they should simply return archive 0, raw resolution
	adjust(&out[0], 0, 10, 10, 800)
	adjust(&out[1], 0, 15, 15, 800)
	adjust(&out[2], 0, 25, 25, 800)
	testPlan(in, rets, out, nil, 800, 0, 0, t)

	t.Run("DifferentPNGroups", func(t *testing.T) {
		// nothing should change
		in[0].PNGroup, out[0].PNGroup = 123, 123
		in[1].PNGroup, out[1].PNGroup = 124, 124
		in[2].PNGroup, out[2].PNGroup = 125, 125
		testPlan(in, rets, out, nil, 800, 0, 0, t)
	})
	t.Run("SamePNGroups", func(t *testing.T) {
		// they should all consolidate to a common interval, the LCM
		// note that in this case it would be more useful to simply read the 2nd rollup for all of them
		in[0].PNGroup, out[0].PNGroup = 123, 123
		in[1].PNGroup, out[1].PNGroup = 123, 123
		in[2].PNGroup, out[2].PNGroup = 123, 123
		adjust(&out[0], 0, 10, 150, 800)
		adjust(&out[1], 0, 15, 150, 800)
		adjust(&out[2], 0, 25, 150, 800)
		testPlan(in, rets, out, nil, 800, 0, 0, t)
	})
}

var result *ReqsPlan

func BenchmarkPlanRequestsSamePNGroupNoLimits(b *testing.B) {
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
		res, _ = planRequests(14*24*3600, reqs, 0, 0, 0)
	}
	result = res
}
