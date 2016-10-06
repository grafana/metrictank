package api

import (
	"errors"
	"net/http"

	"github.com/raintank/metrictank/api/middleware"
	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/api/rbody"
	"github.com/raintank/metrictank/cluster"
)

func (s *Server) getClusterStatus(ctx *middleware.Context) {
	rbody.WriteResponse(ctx, rbody.NewJsonResponse(200, cluster.ThisCluster, ""))
}

func (s *Server) getNodeStatus(ctx *middleware.Context) {
	rbody.WriteResponse(ctx, rbody.NewJsonResponse(200, cluster.ThisNode, ""))
}

func (s *Server) setNodeStatus(ctx *middleware.Context, status models.NodeStatus) {
	cluster.ThisNode.SetPrimary(status.Primary)
	rbody.WriteResponse(ctx, rbody.NewJsonResponse(200, "ok", ""))
}

func (s *Server) appStatus(ctx *middleware.Context) {
	if cluster.ThisNode.IsReady() {
		rbody.WriteResponse(ctx, rbody.NewJsonResponse(200, "ok", ""))
		return
	}

	rbody.WriteResponse(ctx, rbody.NewErrorResponse(http.StatusServiceUnavailable, errors.New("node not ready")))
}
