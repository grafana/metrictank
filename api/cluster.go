package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/metrictank/api/middleware"
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/response"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/stats"
	"github.com/grafana/metrictank/tracing"
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
		Members:     cluster.Manager.MemberList(false, false),
	}
	response.Write(ctx, response.NewJson(200, status, ""))
}

func (s *Server) postClusterMembers(ctx *middleware.Context, req models.ClusterMembers) {
	memberNames := make(map[string]struct{})
	var toJoin []string

	for _, memberNode := range cluster.Manager.MemberList(false, false) {
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

	// query nodes don't own any data
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(200, resp))
		return
	}

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

	// query nodes don't own any data
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(200, &models.IndexTagDetailsResp{}))
		return
	}

	re, err := compileRegexAnchored(req.Filter)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	values := s.MetricIndex.TagDetails(req.OrgId, req.Tag, re)
	response.Write(ctx, response.NewMsgp(200, &models.IndexTagDetailsResp{Values: values}))
}

func (s *Server) indexTags(ctx *middleware.Context, req models.IndexTags) {

	// query nodes don't own any data
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(200, &models.IndexTagsResp{}))
		return
	}

	re, err := compileRegexAnchored(req.Filter)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	tags := s.MetricIndex.Tags(req.OrgId, re)
	response.Write(ctx, response.NewMsgp(200, &models.IndexTagsResp{Tags: tags}))
}

func compileRegexAnchored(pattern string) (*regexp.Regexp, error) {
	if len(pattern) == 0 {
		return nil, nil
	}

	if pattern == "^.+" {
		return nil, nil
	}

	if pattern[0] != '^' {
		pattern = "^(?:" + pattern + ")"
	}
	return regexp.Compile(pattern)
}

func (s *Server) indexAutoCompleteTags(ctx *middleware.Context, req models.IndexAutoCompleteTags) {

	// query nodes don't own any data
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(200, models.StringList(nil)))
		return
	}

	// if there are no expressions given, we can shortcut the evaluation by not using a query
	if len(req.Expr) == 0 {
		tags := s.MetricIndex.FindTags(req.OrgId, req.Prefix, req.Limit)
		response.Write(ctx, response.NewMsgp(200, models.StringList(tags)))
		return
	}

	expressions := req.Expr
	if len(req.Prefix) > 0 {
		expressions = append(expressions, "__tag^="+req.Prefix)
	}

	query, err := tagquery.NewQueryFromStrings(expressions, 0)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	tags := s.MetricIndex.FindTagsWithQuery(req.OrgId, req.Prefix, query, req.Limit)
	response.Write(ctx, response.NewMsgp(200, models.StringList(tags)))
	return
}

func (s *Server) indexAutoCompleteTagValues(ctx *middleware.Context, req models.IndexAutoCompleteTagValues) {

	// query nodes don't own any data
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(200, models.StringList(nil)))
		return
	}

	// if there are no expressions given, we can shortcut the evaluation by not using a query
	if len(req.Expr) == 0 {
		values := s.MetricIndex.FindTagValues(req.OrgId, req.Tag, req.Prefix, req.Limit)
		response.Write(ctx, response.NewMsgp(200, models.StringList(values)))
		return
	}

	expressions := req.Expr
	if len(req.Prefix) > 0 {
		expressions = append(expressions, req.Tag+"^="+req.Prefix)
	}

	query, err := tagquery.NewQueryFromStrings(expressions, 0)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	tags := s.MetricIndex.FindTagValuesWithQuery(req.OrgId, req.Tag, req.Prefix, query, req.Limit)
	response.Write(ctx, response.NewMsgp(200, models.StringList(tags)))
}

func (s *Server) indexTagDelSeries(ctx *middleware.Context, request models.IndexTagDelSeries) {

	res := models.IndexTagDelSeriesResp{}

	// nothing to do on query nodes.
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(200, res))
		return
	}

	for _, path := range request.Paths {
		tags, err := tagquery.ParseTagsFromMetricName(path)
		if err != nil {
			response.Write(ctx, response.WrapErrorForTagDB(err))
			return
		}

		expressions := make(tagquery.Expressions, len(tags))
		builder := strings.Builder{}
		for i := range tags {
			tags[i].StringIntoWriter(&builder)

			expressions[i], err = tagquery.ParseExpression(builder.String())
			if err != nil {
				response.Write(ctx, response.WrapErrorForTagDB(err))
				return
			}
			builder.Reset()
		}

		query, err := tagquery.NewQuery(expressions, 0)
		if err != nil {
			response.Write(ctx, response.WrapErrorForTagDB(err))
			return
		}

		deleted := s.MetricIndex.DeleteTagged(request.OrgId, query)
		res.Count += len(deleted)
	}

	response.Write(ctx, response.NewMsgp(200, res))
}

func (s *Server) IndexTagTerms(ctx *middleware.Context, req models.IndexTagTerms) {
	// query nodes don't own any data.
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(200, &models.GraphiteTagTermsResp{}))
		return
	}

	query, err := tagquery.NewQueryFromStrings(req.Expr, 0)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	total, terms := s.MetricIndex.FindTerms(req.OrgId, req.Tags, query)

	response.Write(ctx, response.NewMsgp(200, &models.GraphiteTagTermsResp{TotalSeries: total, Terms: terms}))
}

func (s *Server) indexFindByTag(ctx *middleware.Context, req models.IndexFindByTag) {

	// query nodes don't own any data.
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(200, &models.IndexFindByTagResp{}))
		return
	}

	query, err := tagquery.NewQueryFromStrings(req.Expr, req.From)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	metrics := s.MetricIndex.FindByTag(req.OrgId, query)
	response.Write(ctx, response.NewMsgp(200, &models.IndexFindByTagResp{Metrics: metrics}))
}

// IndexGet returns a msgp encoded schema.MetricDefinition
func (s *Server) indexGet(ctx *middleware.Context, req models.IndexGet) {

	// query nodes don't own any data.
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgp(404, nil))
		return
	}

	def, ok := s.MetricIndex.Get(req.MKey)
	if !ok {
		response.Write(ctx, response.NewError(http.StatusNotFound, "Not Found"))
		return
	}

	response.Write(ctx, response.NewMsgp(200, &def))
}

// IndexList returns msgp encoded schema.MetricDefinition's
func (s *Server) indexList(ctx *middleware.Context, req models.IndexList) {

	// query nodes don't own any data.
	if s.MetricIndex == nil {
		response.Write(ctx, response.NewMsgpArray(200, nil))
		return
	}

	defs := s.MetricIndex.List(req.OrgId)
	resp := make([]msgp.Marshaler, len(defs))
	for i := range defs {
		d := defs[i]
		resp[i] = &d
	}
	response.Write(ctx, response.NewMsgpArray(200, resp))
}

func (s *Server) getData(ctx *middleware.Context, request models.GetData) {
	var ss models.StorageStats
	series, err := s.getTargetsLocal(ctx.Req.Context(), &ss, request.Requests)
	if err != nil {
		// the only errors returned are from us catching panics, so we should treat them
		// all as internalServerErrors
		log.Errorf("HTTP getData() %s", err.Error())
		response.Write(ctx, response.WrapError(err))
		return
	}
	response.Write(ctx, response.NewMsgp(200, &models.GetDataRespV1{Stats: ss, Series: series}))
}

func (s *Server) indexDelete(ctx *middleware.Context, req models.IndexDelete) {

	// nothing to do on query nodes.
	if s.MetricIndex == nil {
		return
	}

	defs, err := s.metricsDeleteLocal(req.OrgId, req.Query)
	if err != nil {
		// errors can only be caused by bad request.
		response.Write(ctx, response.NewError(http.StatusBadRequest, err.Error()))
		return
	}

	resp := models.MetricsDeleteResp{
		DeletedDefs: defs,
	}
	response.Write(ctx, response.NewMsgp(200, &resp))
}

type PeerResponse struct {
	peer cluster.Node
	buf  []byte
}

// peerQuery takes a request and the path to request it on, then fans it out
// across the cluster, except to the local peer or peers without data.
// Note: unlike the other peerQuery methods, we include peers that are not ready
// If any peer fails requests to
// other peers are aborted.
// ctx:          request context
// data:         request to be submitted
// name:         name to be used in logging & tracing
// path:         path to request on
func (s *Server) peerQuery(ctx context.Context, data cluster.Traceable, name, path string) (map[string]PeerResponse, map[string]error) {

	peers := cluster.Manager.MemberList(false, true)
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
	errors := make(map[string]error)
	for resp := range responses {
		if resp.err != nil {
			errors[resp.data.peer.GetName()] = resp.err
		} else {
			result[resp.data.peer.GetName()] = resp.data
		}
	}

	return result, errors
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
// across the cluster. If any peer fails, we try another replica. If enough
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
		ctx, peerSpan := tracing.NewSpan(ctx, s.Tracer, name+" peerQuery")
		defer peerSpan.Finish()
		defer close(errorChan)
		defer close(resultChan)

		peerGroups, err := cluster.MembersForSpeculativeQuery()
		if err != nil {
			log.Errorf("HTTP peerQuery unable to get peers, %s", err.Error())
			errorChan <- err
			return
		}
		peerSpan.SetTag("peerGroups", len(peerGroups))
		log.Debugf("HTTP %s across %d instances", name, len(peerGroups))

		reqCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		type response struct {
			shardGroup int32
			data       PeerResponse
			err        error
		}

		responses := make(chan response)
		originalPeers := make(map[string]struct{}, len(peerGroups))
		receivedResponses := make(map[int32]struct{}, len(peerGroups))

		askPeer := func(shardGroup int32, peer cluster.Node) {
			log.Debugf("HTTP Render querying %s%s", peer.GetName(), path)
			buf, err := peer.Post(reqCtx, name, path, data)
			if err != nil {
				// log an error, but we will still try other peers for this shardGroup
				log.Errorf("HTTP Render error querying %s%s: %q", peer.GetName(), path, err)
			}

			select {
			case <-reqCtx.Done():
				return
			case responses <- response{shardGroup, PeerResponse{peer, buf}, err}:
				return
			}
		}

		for group, peers := range peerGroups {
			nextPeer := peers[0]
			// shift nextPeer from the group
			peerGroups[group] = peers[1:]

			originalPeers[nextPeer.GetName()] = struct{}{}
			go askPeer(group, nextPeer)
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
			case <-reqCtx.Done():
				//request canceled
				return
			case resp := <-responses:
				if _, ok := receivedResponses[resp.shardGroup]; ok {
					// already received this response (possibly speculatively)
					continue
				}

				if resp.err != nil {
					// check if there is another peer for this shardGroup. If so try it.
					if len(peerGroups[resp.shardGroup]) > 0 {
						speculativeRequests.Inc()
						nextPeer := peerGroups[resp.shardGroup][0]
						// shift nextPeer from the group
						peerGroups[resp.shardGroup] = peerGroups[resp.shardGroup][1:]

						go askPeer(resp.shardGroup, nextPeer)
						continue
					}
					// No more peers to try. Cancel the reqCtx, which will cancel all in-flight
					// requests.
					cancel()
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

						if len(peers) == 0 {
							// no more peers to try
							continue
						}

						nextPeer := peers[0]
						// shift nextPeer from the group
						peerGroups[shardGroup] = peers[1:]

						// send the request to the next peer for the group.
						speculativeRequests.Inc()
						go askPeer(shardGroup, nextPeer)
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
