package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"

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
		NodeName:    cluster.Manager.ThisNode().GetName(),
		Members:     cluster.Manager.MemberList(),
	}
	response.Write(ctx, response.NewJson(200, status, ""))
}

func (s *Server) postClusterMembers(ctx *middleware.Context, req models.ClusterMembers) {
	memberNames := make(map[string]struct{})
	var toJoin []string

	for _, memberNode := range cluster.Manager.MemberList() {
		memberNames[memberNode.GetName()] = struct{}{}
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

func (s *Server) indexTagDetails(ctx *middleware.Context, req models.IndexTagDetails) {
	values, err := s.MetricIndex.TagDetails(req.OrgId, req.Tag, req.Filter, req.From)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}
	response.Write(ctx, response.NewMsgp(200, &models.IndexTagDetailsResp{Values: values}))
}

func (s *Server) indexTags(ctx *middleware.Context, req models.IndexTags) {
	tags, err := s.MetricIndex.Tags(req.OrgId, req.Filter, req.From)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}
	response.Write(ctx, response.NewMsgp(200, &models.IndexTagsResp{Tags: tags}))
}

func (s *Server) indexAutoCompleteTags(ctx *middleware.Context, req models.IndexAutoCompleteTags) {
	tags, err := s.MetricIndex.FindTags(req.OrgId, req.Prefix, req.Expr, req.From, req.Limit)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}
	response.Write(ctx, response.NewMsgp(200, models.StringList(tags)))
}

func (s *Server) indexAutoCompleteTagValues(ctx *middleware.Context, req models.IndexAutoCompleteTagValues) {
	tags, err := s.MetricIndex.FindTagValues(req.OrgId, req.Tag, req.Prefix, req.Expr, req.From, req.Limit)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}
	response.Write(ctx, response.NewMsgp(200, models.StringList(tags)))
}

func (s *Server) indexTagDelSeries(ctx *middleware.Context, request models.IndexTagDelSeries) {
	deleted, err := s.MetricIndex.DeleteTagged(request.OrgId, request.Paths)
	if err != nil {
		response.Write(ctx, response.WrapErrorForTagDB(err))
		return
	}

	res := models.IndexTagDelSeriesResp{}
	res.Count = len(deleted)

	response.Write(ctx, response.NewMsgp(200, res))
}

func (s *Server) indexFindByTag(ctx *middleware.Context, req models.IndexFindByTag) {
	metrics, err := s.MetricIndex.FindByTag(req.OrgId, req.Expr, req.From)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}
	response.Write(ctx, response.NewMsgp(200, &models.IndexFindByTagResp{Metrics: metrics}))
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

type PeerResponse struct {
	peer cluster.Node
	buf  []byte
}

// peerQuery takes a request and the path to request it on, then fans it out
// across the cluster, except to the local peer. If any peer fails requests to
// other peers are aborted.
// ctx:          request context
// data:         request to be submitted
// name:         name to be used in logging & tracing
// path:         path to request on
func (s *Server) peerQuery(ctx context.Context, data cluster.Traceable, name, path string, allPeers bool) (map[string]PeerResponse, error) {
	var peers []cluster.Node
	var err error

	if allPeers {
		peers = cluster.Manager.MemberList()
	} else {
		peers, err = cluster.MembersForQuery()
		if err != nil {
			log.Error(3, "HTTP peerQuery unable to get peers, %s", err)
			return nil, err
		}
	}
	log.Debug("HTTP %s across %d instances", name, len(peers)-1)

	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	responses := make(chan struct {
		data PeerResponse
		err  error
	}, 1)
	var wg sync.WaitGroup
	for _, peer := range peers {
		if peer.IsLocal() {
			continue
		}
		wg.Add(1)
		go func(peer cluster.Node) {
			defer wg.Done()
			log.Debug("HTTP Render querying %s%s", peer.GetName(), path)
			buf, err := peer.Post(reqCtx, name, path, data)
			if err != nil {
				cancel()
				log.Error(4, "HTTP Render error querying %s%s: %q", peer.GetName(), path, err)
			}
			responses <- struct {
				data PeerResponse
				err  error
			}{PeerResponse{peer, buf}, err}
		}(peer)
	}
	// wait for all list goroutines to end, then close our responses channel
	go func() {
		wg.Wait()
		close(responses)
	}()

	result := make(map[string]PeerResponse)
	for resp := range responses {
		if resp.err != nil {
			return nil, err
		}
		result[resp.data.peer.GetName()] = resp.data
	}

	return result, nil
}
