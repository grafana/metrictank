package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/grafana/metrictank/api/middleware"
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/response"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/stats"
	log "github.com/sirupsen/logrus"
	"github.com/tinylib/msgp/msgp"
)

var NotFoundErr = errors.New("not found")

var (

	// metric api.cluster.speculative.attempts is how many peer queries resulted in speculation
	speculativeAttempts = stats.NewCounter32("api.cluster.speculative.attempts")

	// metric api.cluster.speculative.wins is how many peer queries were improved due to speculation
	speculativeWins = stats.NewCounter32("api.cluster.speculative.wins")

	// metric api.cluster.speculative.requests is how many speculative http requests made to peers
	speculativeRequests = stats.NewCounter32("api.cluster.speculative.requests")
)

func (s *Server) explainPriority(ctx *middleware.Context) {
	var data []interface{}
	for _, p := range s.prioritySetters {
		data = append(data, p.ExplainPriority())
	}
	response.Write(ctx, response.NewJson(200, data, ""))
}

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
			response.Write(ctx, response.WrapError(err))
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
	def, ok := s.MetricIndex.Get(req.MKey)
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
		log.Errorf("HTTP getData() %s", err.Error())
		response.Write(ctx, response.WrapError(err))
		return
	}
	response.Write(ctx, response.NewMsgp(200, &models.GetDataResp{Series: series}))
}

func (s *Server) indexDelete(ctx *middleware.Context, req models.IndexDelete) {
	deleted, err := s.MetricIndex.Delete(req.OrgId, req.Query)
	if err != nil {
		// errors can only be caused by bad request.
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	resp := models.MetricsDeleteResp{
		DeletedDefs: deleted,
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
			log.Errorf("HTTP peerQuery unable to get peers, %s", err.Error())
			return nil, err
		}
	}
	log.Debugf("HTTP %s across %d instances", name, len(peers)-1)

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
			log.Debugf("HTTP Render querying %s%s", peer.GetName(), path)
			buf, err := peer.Post(reqCtx, name, path, data)
			if err != nil {
				cancel()
				log.Errorf("HTTP Render error querying %s%s: %q", peer.GetName(), path, err.Error())
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
			return nil, resp.err
		}
		result[resp.data.peer.GetName()] = resp.data
	}

	return result, nil
}

// peerQuerySpeculative takes a request and the path to request it on, then fans it out
// across the cluster. If any peer fails requests to other peers are aborted. If enough
// peers have been heard from (based on speculation-threshold configuration), and we
// are missing the others, try to speculatively query each other member of each shard group.
// ctx:          request context
// data:         request to be submitted
// name:         name to be used in logging & tracing
// path:         path to request on
func (s *Server) peerQuerySpeculative(ctx context.Context, data cluster.Traceable, name, path string) (map[string]PeerResponse, error) {
	result := make(map[string]PeerResponse)

	responseChan, errorChan := s.peerQuerySpeculativeChan(ctx, data, name, path)

	for resp := range responseChan {
		result[resp.peer.GetName()] = resp
	}

	err := <-errorChan
	return result, err
}

// peerQuerySpeculativeChan takes a request and the path to request it on, then fans it out
// across the cluster. If any peer fails requests to other peers are aborted. If enough
// peers have been heard from (based on speculation-threshold configuration), and we
// are missing the others, try to speculatively query other members of the shard group.
// ctx:          request context
// data:         request to be submitted
// name:         name to be used in logging & tracing
// path:         path to request on
// resultChan:   channel to put responses on as they come in
func (s *Server) peerQuerySpeculativeChan(ctx context.Context, data cluster.Traceable, name, path string) (<-chan PeerResponse, <-chan error) {
	resultChan := make(chan PeerResponse)
	errorChan := make(chan error, 1)

	go func() {
		defer close(errorChan)
		defer close(resultChan)

		peerGroups, err := cluster.MembersForSpeculativeQuery()
		if err != nil {
			log.Errorf("HTTP peerQuery unable to get peers, %s", err.Error())
			errorChan <- err
			return
		}
		log.Debugf("HTTP %s across %d instances", name, len(peerGroups)-1)

		reqCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		originalPeers := make(map[string]struct{}, len(peerGroups))
		receivedResponses := make(map[int32]struct{}, len(peerGroups))

		responses := make(chan struct {
			shardGroup int32
			data       PeerResponse
			err        error
		}, 1)

		askPeer := func(shardGroup int32, peer cluster.Node) {
			log.Debugf("HTTP Render querying %s%s", peer.GetName(), path)
			buf, err := peer.Post(reqCtx, name, path, data)

			select {
			case <-ctx.Done():
				return
			default:
				// Not canceled, continue
			}

			if err != nil {
				cancel()
				log.Errorf("HTTP Render error querying %s%s: %q", peer.GetName(), path, err)
			}
			responses <- struct {
				shardGroup int32
				data       PeerResponse
				err        error
			}{shardGroup, PeerResponse{peer, buf}, err}
		}

		for group, peers := range peerGroups {
			peer := peers[0]
			originalPeers[peer.GetName()] = struct{}{}
			go askPeer(group, peer)
		}

		var ticker *time.Ticker
		var tickChan <-chan time.Time
		if speculationThreshold != 1 {
			ticker = time.NewTicker(5 * time.Millisecond)
			tickChan = ticker.C
			defer ticker.Stop()
		}

		for len(receivedResponses) < len(peerGroups) {
			select {
			case <-ctx.Done():
				//request canceled
				return
			case resp := <-responses:
				if _, ok := receivedResponses[resp.shardGroup]; ok {
					// already received this response (possibly speculatively)
					continue
				}

				if resp.err != nil {
					errorChan <- resp.err
					return
				}

				resultChan <- resp.data
				receivedResponses[resp.shardGroup] = struct{}{}
				delete(originalPeers, resp.data.peer.GetName())

			case <-tickChan:
				// Check if it's time to speculate!
				percentReceived := float64(len(receivedResponses)) / float64(len(peerGroups))
				if percentReceived >= speculationThreshold {
					// kick off speculative queries to other members now
					ticker.Stop()
					speculativeAttempts.Inc()
					for shardGroup, peers := range peerGroups {
						if _, ok := receivedResponses[shardGroup]; ok {
							continue
						}
						eligiblePeers := peers[1:]
						for _, peer := range eligiblePeers {
							speculativeRequests.Inc()
							go askPeer(shardGroup, peer)
						}
					}
				}
			}
		}

		if len(originalPeers) > 0 {
			speculativeWins.Inc()
		}
	}()

	return resultChan, errorChan
}
