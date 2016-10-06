package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/raintank/metrictank/api/middleware"
	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/api/rbody"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/tinylib/msgp/msgp"
)

var MissingPatternErr = errors.New("missing pattern arg")
var MissingOrgIdErr = errors.New("missing orgId arg")
var MissingIdErr = errors.New("missing Id arg")
var NotFoundErr = errors.New("not found")

// IndexFind returns a sequence of msgp encoded idx.Node's
func (s *Server) indexFind(ctx *middleware.Context, req models.IndexFind) {
	if len(req.Patterns) == 0 {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, MissingPatternErr))
		return
	}
	if req.OrgId == 0 {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, MissingOrgIdErr))
		return
	}

	// metricDefs only get updated periodically (when using CassandraIdx), so we add a 1day (86400seconds) buffer when
	// filtering by our From timestamp.  This should be moved to a configuration option,
	// but that will require significant refactoring to expose the updateInterval used
	// in the MetricIdx.  So this will have to do for now.
	if req.From != 0 {
		req.From -= 86400
	}
	resp := models.NewIndexFindResp()

	for _, pattern := range req.Patterns {
		nodes, err := s.MetricIndex.Find(req.OrgId, pattern, req.From)
		if err != nil {
			rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, err))
			return
		}
		resp.Nodes[pattern] = nodes
	}
	rbody.WriteResponse(ctx, rbody.NewMsgpResponse(200, resp))
}

// IndexGet returns a msgp encoded schema.MetricDefinition
func (s *Server) indexGet(ctx *middleware.Context, req models.IndexGet) {
	if req.Id == "" {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, MissingIdErr))
		return
	}
	def, err := s.MetricIndex.Get(req.Id)
	if err != nil {
	} else { // currently this can only be notFound
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusNotFound, NotFoundErr))
		return
	}

	rbody.WriteResponse(ctx, rbody.NewMsgpResponse(200, &def))
}

// IndexList returns msgp encoded schema.MetricDefinition's
func (s *Server) indexList(ctx *middleware.Context, req models.IndexList) {
	if req.OrgId == 0 {
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusBadRequest, MissingOrgIdErr))
		return
	}
	defs := s.MetricIndex.List(req.OrgId)
	resp := make([]msgp.Marshaler, len(defs))
	for i, _ := range defs {
		d := defs[i]
		resp[i] = &d
	}
	rbody.WriteResponse(ctx, rbody.NewMsgpArrayResponse(200, resp))
}

func (s *Server) getData(ctx *middleware.Context, request models.GetData) {
	pre := time.Now()

	series, err := s.getTargets(request.Requests)
	if err != nil {
		log.Error(3, "HTTP getData() %s", err.Error())
		rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusInternalServerError, err))
		return
	}
	reqHandleDuration.Value(time.Now().Sub(pre))
	rbody.WriteResponse(ctx, rbody.NewMsgpResponse(200, &models.GetDataResp{Series: series}))
}
