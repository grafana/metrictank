package api

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/grafana/metrictank/api/middleware"
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/response"
	"github.com/grafana/metrictank/cluster"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/tinylib/msgp/msgp"
)

var NotFoundErr = errors.New("not found")

func (s *Server) getNodeStatus(ctx *middleware.Context) {
	response.Write(ctx, response.NewJson(200, cluster.Manager.ThisNode(), ""))
}

func (s *Server) setNodeStatus(ctx *middleware.Context, status models.NodeStatus) {
	primary, err := strconv.ParseBool(status.Primary)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, fmt.Sprintf(
			"could not parse status to bool. %s",
			err.Error())),
		)
		return
	}
	cluster.Manager.SetPrimary(primary)
	ctx.PlainText(200, []byte("OK"))
}

func (s *Server) appStatus(ctx *middleware.Context) {
	if cluster.Manager.IsReady() {
		ctx.PlainText(200, []byte("OK"))
		return
	}

	response.Write(ctx, response.NewError(http.StatusServiceUnavailable, "node not ready"))
}

func (s *Server) getClusterStatus(ctx *middleware.Context) {
	status := models.ClusterStatus{
		ClusterName: cluster.ClusterName,
		NodeName:    cluster.Manager.ThisNode().Name,
		Members:     cluster.Manager.MemberList(),
	}
	response.Write(ctx, response.NewJson(200, status, ""))
}

func (s *Server) postClusterMembers(ctx *middleware.Context, req models.ClusterMembers) {
	memberNames := make(map[string]struct{})
	var toJoin []string

	for _, memberNode := range cluster.Manager.MemberList() {
		memberNames[memberNode.Name] = struct{}{}
	}

	for _, peerName := range req.Members {
		if _, ok := memberNames[peerName]; !ok {
			toJoin = append(toJoin, peerName)
		}
	}

	resp := models.ClusterMembersResp{
		Status:       "ok",
		MembersAdded: 0,
	}

	if len(toJoin) == 0 {
		response.Write(ctx, response.NewJson(200, resp, ""))
		return
	}

	n, err := cluster.Manager.Join(toJoin)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, fmt.Sprintf(
			"error when joining cluster members: %s", err.Error())),
		)
		return
	}
	resp.MembersAdded = n
	response.Write(ctx, response.NewJson(200, resp, ""))
}

// IndexFind returns a sequence of msgp encoded idx.Node's
func (s *Server) indexFind(ctx *middleware.Context, req models.IndexFind) {
	// metricDefs only get updated periodically (when using CassandraIdx), so we add a 1day (86400seconds) buffer when
	// filtering by our From timestamp.  This should be moved to a configuration option
	if req.From != 0 {
		req.From -= 86400
	}
	resp := models.NewIndexFindResp()

	for _, pattern := range req.Patterns {
		nodes, err := s.MetricIndex.Find(req.OrgId, pattern, req.From)
		if err != nil {
			response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
			return
		}
		resp.Nodes[pattern] = nodes
	}
	response.Write(ctx, response.NewMsgp(200, resp))
}

func (s *Server) indexTag(ctx *middleware.Context, req models.IndexTag) {
	values := s.MetricIndex.Tag(req.OrgId, req.Tag, 0)
	response.Write(ctx, response.NewMsgp(200, &models.IndexTagResp{Values: values}))
}

func (s *Server) indexTagList(ctx *middleware.Context, req models.IndexTagList) {
	tags, err := s.MetricIndex.TagList(req.OrgId, req.Filter, req.From)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}
	response.Write(ctx, response.NewMsgp(200, &models.IndexTagListResp{Tags: tags}))
}

func (s *Server) indexTagFindSeries(ctx *middleware.Context, req models.IndexTagFindSeries) {
	ids, err := s.MetricIndex.FindByTag(req.OrgId, req.Expressions, 0)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}
	idStrings := make([]string, 0, len(ids))
	for id := range ids {
		idStrings = append(idStrings, id.String())
	}
	response.Write(ctx, response.NewMsgp(200, &models.IndexTagFindSeriesResp{Series: idStrings}))
}

// IndexGet returns a msgp encoded schema.MetricDefinition
func (s *Server) indexGet(ctx *middleware.Context, req models.IndexGet) {
	def, ok := s.MetricIndex.Get(req.Id)
	if !ok {
		response.Write(ctx, response.NewError(http.StatusNotFound, "Not Found"))
		return
	}

	response.Write(ctx, response.NewMsgp(200, &def))
}

// IndexList returns msgp encoded schema.MetricDefinition's
func (s *Server) indexList(ctx *middleware.Context, req models.IndexList) {
	defs := s.MetricIndex.List(req.OrgId)
	resp := make([]msgp.Marshaler, len(defs))
	for i := range defs {
		d := defs[i]
		resp[i] = &d
	}
	response.Write(ctx, response.NewMsgpArray(200, resp))
}

func (s *Server) getData(ctx *middleware.Context, request models.GetData) {
	series, err := s.getTargetsLocal(ctx.Req.Context(), request.Requests)
	if err != nil {
		// the only errors returned are from us catching panics, so we should treat them
		// all as internalServerErrors
		log.Error(3, "HTTP getData() %s", err.Error())
		response.Write(ctx, response.WrapError(err))
		return
	}
	response.Write(ctx, response.NewMsgp(200, &models.GetDataResp{Series: series}))
}

func (s *Server) indexDelete(ctx *middleware.Context, req models.IndexDelete) {
	defs, err := s.MetricIndex.Delete(req.OrgId, req.Query)
	if err != nil {
		// errors can only be caused by bad request.
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	resp := models.MetricsDeleteResp{
		DeletedDefs: len(defs),
	}
	response.Write(ctx, response.NewMsgp(200, &resp))
}
