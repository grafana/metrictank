package api

import (
	"net/http"

	"github.com/Unknwon/macaron"
	"github.com/raintank/metrictank/api/models"
)

func (s *Server) getClusterStatus(ctx macaron.Context) {
	ctx.JSON(200, "ok")
}

func (s *Server) setClusterStatus(ctx macaron.Context, status models.ClusterStatus) {
	ctx.JSON(200, "ok")
}

func (s *Server) appStatus(ctx macaron.Context) {
	if s.ClusterMgr.IsReady() {
		ctx.JSON(200, "ok")
		return
	}

	ctx.JSON(http.StatusServiceUnavailable, "not ready")
}
