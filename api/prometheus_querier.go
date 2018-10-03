package api

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/util"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	log "github.com/sirupsen/logrus"
)

// Querier creates a new querier that will operate on the subject server
// it needs the org-id stored in a context value
func (s *Server) Querier(ctx context.Context, min, max int64) (storage.Querier, error) {
	from := uint32(min / 1000)
	to := uint32(max / 1000)
	return NewQuerier(ctx, s, from, to, ctx.Value(orgID("org-id")).(uint32), false), nil
}

// querier implements Prometheus' Querier interface
type querier struct {
	*Server
	from         uint32
	to           uint32
	OrgID        uint32
	metadataOnly bool
	ctx          context.Context
}

func NewQuerier(ctx context.Context, s *Server, from, to uint32, orgId uint32, metadataOnly bool) storage.Querier {
	return &querier{
		s,
		from,
		to,
		orgId,
		metadataOnly,
		ctx,
	}
}

// Select returns a set of series that matches the given label matchers.
func (q *querier) Select(matchers ...*labels.Matcher) (storage.SeriesSet, error) {
	minFrom := uint32(math.MaxUint32)
	var maxTo uint32
	var target string
	var reqs []models.Req

	expressions := []string{}
	for _, matcher := range matchers {
		if matcher.Name == model.MetricNameLabel {
			matcher.Name = "name"
		}
		if matcher.Type == labels.MatchNotRegexp {
			expressions = append(expressions, fmt.Sprintf("%s!=~%s", matcher.Name, matcher.Value))
		} else {
			expressions = append(expressions, fmt.Sprintf("%s%s%s", matcher.Name, matcher.Type, matcher.Value))
		}
	}

	series, err := q.clusterFindByTag(q.ctx, q.OrgID, expressions, 0)
	if err != nil {
		return nil, err
	}

	if q.metadataOnly {
		return BuildMetadataSeriesSet(series)
	}

	minFrom = util.Min(minFrom, q.from)
	maxTo = util.Max(maxTo, q.to)
	for _, s := range series {
		for _, metric := range s.Series {
			for _, archive := range metric.Defs {
				consReq := consolidation.None
				fn := mdata.Aggregations.Get(archive.AggId).AggregationMethod[0]
				cons := consolidation.Consolidator(fn)

				newReq := models.NewReq(archive.Id, archive.NameWithTags(), target, q.from, q.to, math.MaxUint32, uint32(archive.Interval), cons, consReq, s.Node, archive.SchemaId, archive.AggId)
				reqs = append(reqs, newReq)
			}
		}
	}

	select {
	case <-q.ctx.Done():
		//request canceled
		return nil, fmt.Errorf("request canceled")
	default:
	}

	reqRenderSeriesCount.Value(len(reqs))
	if len(reqs) == 0 {
		return nil, fmt.Errorf("no series found")
	}

	// note: if 1 series has a movingAvg that requires a long time range extension, it may push other reqs into another archive. can be optimized later
	reqs, _, _, err = alignRequests(uint32(time.Now().Unix()), minFrom, maxTo, reqs)
	if err != nil {
		log.Errorf("HTTP Render alignReq error: %s", err.Error())
		return nil, err
	}

	out, err := q.getTargets(q.ctx, reqs)
	if err != nil {
		log.Errorf("HTTP Render %s", err.Error())
		return nil, err
	}

	return SeriesToSeriesSet(out)
}

// LabelValues returns all potential values for a label name.
func (q *querier) LabelValues(name string) ([]string, error) {
	expressions := []string{"name=~[a-zA-Z_][a-zA-Z0-9_]*$"}
	if name == model.MetricNameLabel {
		name = "name"
		expressions = append(expressions, "name=~[a-zA-Z_:][a-zA-Z0-9_:]*$")
	}
	return q.MetricIndex.FindTagValues(q.OrgID, name, "", expressions, 0, 100000)
}

// Close releases the resources of the Querier.
func (q *querier) Close() error {
	return nil
}
