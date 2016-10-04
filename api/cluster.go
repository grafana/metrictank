package api

import (
	"net/http"

	"github.com/raintank/metrictank/api/middleware"
	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/cluster"
)

func (s *Server) getClusterStatus(ctx *middleware.Context) {
	ctx.JSON(200, cluster.ThisCluster)
}

func (s *Server) getNodeStatus(ctx *middleware.Context) {
	ctx.JSON(200, cluster.ThisNode)
}

func (s *Server) setNodeStatus(ctx *middleware.Context, status models.NodeStatus) {
	cluster.ThisNode.SetPrimary(status.Primary)
	ctx.JSON(200, "ok")
}

func (s *Server) appStatus(ctx *middleware.Context) {
	if cluster.ThisNode.IsReady() {
		ctx.JSON(200, "ok")
		return
	}

	ctx.JSON(http.StatusServiceUnavailable, "not ready")
}
