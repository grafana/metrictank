package api

import (
	"net/http"
	"time"

	"github.com/raintank/metrictank/api/middleware"
	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/api/rbody"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/worldping-api/pkg/log"
)

// IndexFind returns a sequence of msgp encoded idx.Node's
func (s *Server) indexFind(ctx *middleware.Context, req models.IndexFind) {
	if req.Pattern == "" {
		ctx.Error(http.StatusBadRequest, "missing pattern arg")
		return
	}
	if req.OrgId == 0 {
		ctx.Error(http.StatusBadRequest, "missing orgId arg")
		return
	}

	// metricDefs only get updated periodically (when using CassandraIdx), so we add a 1day (86400seconds) buffer when
	// filtering by our From timestamp.  This should be moved to a configuration option,
	// but that will require significant refactoring to expose the updateInterval used
	// in the MetricIdx.  So this will have to do for now.
	if req.From != 0 {
		req.From -= 86400
	}

	var buf []byte
	nodes, err := s.MetricIndex.Find(req.OrgId, req.Pattern, req.From)
	if err != nil {
		ctx.Error(http.StatusBadRequest, err.Error())
		return
	}
	for _, node := range nodes {
		buf, err = node.MarshalMsg(buf)
		if err != nil {
			log.Error(4, "HTTP IndexFind() marshal err: %q", err)
			ctx.Error(http.StatusInternalServerError, err.Error())
			return
		}
	}
	rbody.WriteResponse(ctx, buf, rbody.HttpTypeMsgp, "")
}

// IndexGet returns a msgp encoded schema.MetricDefinition
func (s *Server) indexGet(ctx *middleware.Context, req models.IndexGet) {
	if req.Id == "" {
		ctx.Error(http.StatusBadRequest, "missing ID")
		return
	}
	var buf []byte
	def, err := s.MetricIndex.Get(req.Id)
	if err == nil {
		buf, err = def.MarshalMsg(buf)
		if err != nil {
			log.Error(4, "HTTP IndexGet() marshal err: %q", err)
			ctx.Error(http.StatusInternalServerError, err.Error())
			return
		}
	} else { // currently this can only be notFound
		ctx.Error(http.StatusNotFound, "not found")
		return
	}

	rbody.WriteResponse(ctx, buf, rbody.HttpTypeMsgp, "")
}

// IndexList returns msgp encoded schema.MetricDefinition's
func (s *Server) indexList(ctx *middleware.Context, req models.IndexList) {
	if req.OrgId == 0 {
		ctx.Error(http.StatusBadRequest, "missing org arg")
		return
	}
	var err error
	var buf []byte
	defs := s.MetricIndex.List(req.OrgId)
	for _, def := range defs {
		buf, err = def.MarshalMsg(buf)
		if err != nil {
			log.Error(4, "HTTP IndexList() marshal err: %q", err)
			ctx.Error(http.StatusInternalServerError, err.Error())
			return
		}
	}
	rbody.WriteResponse(ctx, buf, rbody.HttpTypeMsgp, "")
}

func (s *Server) getData(ctx *middleware.Context, req models.Req) {
	pre := time.Now()

	req.Node = cluster.ThisNode
	points, interval, err := s.getTarget(req)
	if err != nil {
		log.Error(3, "HTTP getData() %s", err.Error())
		ctx.Error(http.StatusInternalServerError, err.Error())
		return
	}
	series := models.Series{
		Target:     req.Target,
		Datapoints: points,
		Interval:   interval,
	}

	var buf []byte
	buf, err = series.MarshalMsg(buf)
	if err != nil {
		log.Error(3, "HTTP getData() %s", err.Error())
		ctx.Error(http.StatusInternalServerError, err.Error())
		return
	}

	reqHandleDuration.Value(time.Now().Sub(pre))
	rbody.WriteResponse(ctx, buf, rbody.HttpTypeMsgp, "")
}
