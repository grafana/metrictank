package api

import (
	"net/http"

	"github.com/raintank/metrictank/api/middleware"
	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/cluster"
)

func (s *Server) getClusterStatus(ctx *middleware.Context) {
	ctx.JSON(200, cluster.ThisCluster.Self)
}

func (s *Server) setClusterStatus(ctx *middleware.Context, status models.ClusterStatus) {
	cluster.ThisCluster.SetPrimary(status.Primary)
	ctx.JSON(200, "ok")
}

func (s *Server) appStatus(ctx *middleware.Context) {
	if s.ClusterMgr.IsReady() {
		ctx.JSON(200, "ok")
		return
	}

	ctx.JSON(http.StatusServiceUnavailable, "not ready")
}
