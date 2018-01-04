package api

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/grafana/metrictank/api/middleware"
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/response"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/idx"
	"github.com/raintank/worldping-api/pkg/log"
)

func (s *Server) ccacheDelete(ctx *middleware.Context, req models.CCacheDelete) {
	res := models.CCacheDeleteResp{}
	code := 200

	if req.Propagate {
		res.Peers = s.ccacheDeletePropagate(ctx.Req.Context(), &req)
		for _, peer := range res.Peers {
			if peer.Errors > 0 {
				code = 500
			}
		}
	}

	fullFlush := false
	for _, pattern := range req.Patterns {
		if pattern == "**" {
			fullFlush = true
		}
	}

	if fullFlush {
		delSeries, delArchives := s.Cache.Reset()
		res.DeletedSeries += delSeries
		res.DeletedArchives += delArchives
	} else {
		var toClear []idx.Node
		if len(req.Patterns) > 0 {
			for _, pattern := range req.Patterns {
				nodes, err := s.MetricIndex.Find(req.OrgId, pattern, 0)
				if err != nil {
					if res.Errors == 0 {
						res.FirstError = err.Error()
					}
					res.Errors += 1
					code = 500
				} else {
					toClear = append(toClear, nodes...)
				}
			}
		}

		if len(req.Expr) > 0 {
			nodes, err := s.MetricIndex.FindByTag(req.OrgId, req.Expr, 0)
			if err != nil {
				if res.Errors == 0 {
					res.FirstError = err.Error()
				}
				res.Errors += 1
				code = 500
			} else {
				toClear = append(toClear, nodes...)
			}
		}

		if len(toClear) > 0 {
			for _, node := range toClear {
				for _, def := range node.Defs {
					delSeries, delArchives := s.Cache.DelMetric(def.Id)
					res.DeletedSeries += delSeries
					res.DeletedArchives += delArchives
				}
			}
		}
	}
	response.Write(ctx, response.NewJson(code, res, ""))
}

func (s *Server) ccacheDeletePropagate(ctx context.Context, req *models.CCacheDelete) map[string]models.CCacheDeleteResp {
	// we never want to propagate more than once to avoid loops
	req.Propagate = false

	peers := cluster.Manager.MemberList()
	peerResults := make(map[string]models.CCacheDeleteResp)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, peer := range peers {
		if peer.IsLocal() {
			continue
		}
		wg.Add(1)
		go func(peer cluster.Node) {
			mu.Lock()
			defer mu.Unlock()
			peerResults[peer.GetName()] = s.ccacheDeleteRemote(ctx, req, peer)
			wg.Done()
		}(peer)
	}
	wg.Wait()

	return peerResults
}

func (s *Server) ccacheDeleteRemote(ctx context.Context, req *models.CCacheDelete, peer cluster.Node) models.CCacheDeleteResp {
	var res models.CCacheDeleteResp

	log.Debug("HTTP metricDelete calling %s/ccache/delete", peer.GetName())
	buf, err := peer.Post(ctx, "ccacheDeleteRemote", "/ccache/delete", *req)
	if err != nil {
		log.Error(4, "HTTP ccacheDelete error querying %s/ccache/delete: %q", peer.GetName(), err)
		res.FirstError = err.Error()
		res.Errors++
		return res
	}

	err = json.Unmarshal(buf, &res)
	if err != nil {
		log.Error(4, "HTTP ccacheDelete error unmarshaling body from %s/ccache/delete: %q", peer.GetName(), err)
		res.FirstError = err.Error()
		res.Errors++
	}

	return res
}
