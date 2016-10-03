package api

import (
	"net/http"

	"github.com/raintank/metrictank/api/middleware"
	"github.com/raintank/metrictank/api/models"
)

func (s *Server) getClusterStatus(ctx *middleware.Context) {
	ctx.JSON(200, "ok")
}

func (s *Server) setClusterStatus(ctx *middleware.Context, status models.ClusterStatus) {
	ctx.JSON(200, "ok")
}

func (s *Server) appStatus(ctx *middleware.Context) {
	if s.ClusterMgr.IsReady() {
		ctx.JSON(200, "ok")
		return
	}

	ctx.JSON(http.StatusServiceUnavailable, "not ready")
}
