package api

import (
	"github.com/grafana/metrictank/api/middleware"
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/response"
	"github.com/grafana/metrictank/input/kafkamdm"
)

func (s *Server) getStartupGCPercent(ctx *middleware.Context) {
	response.Write(ctx, response.NewJson(200, models.StartupGCPercent{Value: kafkamdm.StartupGCPercent}, ""))
}

func (s *Server) getNormalGCPercent(ctx *middleware.Context) {
	response.Write(ctx, response.NewJson(200, models.NormalGCPercent{Value: kafkamdm.NormalGCPercent}, ""))
}

func (s *Server) setStartupGCPercent(ctx *middleware.Context, percent models.StartupGCPercent) {
	kafkamdm.GoGCmutex.Lock()
	defer kafkamdm.GoGCmutex.Unlock()
	kafkamdm.StartupGCPercent = percent.Value
	ctx.PlainText(200, []byte("OK"))
}

func (s *Server) setNormalGCPercent(ctx *middleware.Context, percent models.NormalGCPercent) {
	kafkamdm.GoGCmutex.Lock()
	defer kafkamdm.GoGCmutex.Unlock()
	kafkamdm.NormalGCPercent = percent.Value
	ctx.PlainText(200, []byte("OK"))
}
