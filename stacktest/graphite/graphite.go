package graphite

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

var renderClient *http.Client

func init() {
	renderClient = &http.Client{
		// definitely works as timeout in waiting for response, not sure re dial timeout
		// note : MT's internal cross-cluster timeouts are 5s, so we have to wait at least as long to classify properly
		Timeout: time.Second * 6,
	}
}

func RequestForLocalTestingGraphite(target, from string) *http.Request {
	return RequestForLocalTesting("http://localhost", target, from)
}

func RequestForLocalTestingGraphite8080(target, from string) *http.Request {
	return RequestForLocalTesting("http://localhost:8080", target, from)
}

func RequestForLocalTestingMT(target, from string) *http.Request {
	req := RequestForLocalTesting("http://localhost:6060", target, from)
	req.Header.Add("X-Org-Id", "1")
	return req
}

func RequestForLocalTesting(base, target, from string) *http.Request {
	url := fmt.Sprintf("%s/render?target=%s&format=json&from=%s", base, target, from)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}

	return req
}

func ExecuteRenderQuery(req *http.Request) Response {
	var r Response
	resp, err := renderClient.Do(req)
	if err != nil {
		r.HTTPErr = err
		return r
	}
	r.Code = resp.StatusCode
	traceHeader := resp.Header["Trace-Id"]
	if len(traceHeader) > 0 {
		r.TraceID = traceHeader[0]
	}
	r.DecodeErr = json.NewDecoder(resp.Body).Decode(&r.Decoded)
	resp.Body.Close()
	return r
}

func Retry(req *http.Request, times int, validate Validator) (Response, bool) {
	var resp Response
	for i := 0; i < times; i++ {
		if i > 0 {
			time.Sleep(time.Second)
		}
		resp = ExecuteRenderQuery(req)
		if validate(resp) {
			return resp, true
		}
	}
	return resp, false
}

// temporary check results
type checkResultsTemp struct {
	sync.Mutex
	valid []int // each position corresponds to a validator
	// categories of invalid responses
	empty      int
	timeout    int
	other      int
	firstOther *Response
}

// final outcome of check results
type CheckResults struct {
	Valid []int // each position corresponds to a validator
	// categories of invalid responses
	Empty      int
	Timeout    int
	Other      int
	FirstOther *Response
}

func newCheckResultsTemp(validators []Validator) *checkResultsTemp {
	return &checkResultsTemp{
		valid: make([]int, len(validators)),
	}
}

func checkWorker(req *http.Request, wg *sync.WaitGroup, cr *checkResultsTemp, validators []Validator) {
	r := ExecuteRenderQuery(req)
	defer wg.Done()
	for i, v := range validators {
		if v(r) {
			cr.Lock()
			cr.valid[i] += 1
			cr.Unlock()
			return
		}
	}
	// if not valid, try to categorize in the common buckets, or fall back to 'other'
	if r.HTTPErr == nil && r.DecodeErr == nil && len(r.Decoded) == 0 {
		cr.Lock()
		cr.empty += 1
		cr.Unlock()
		return
	}
	if r.HTTPErr != nil {
		if err2, ok := r.HTTPErr.(*url.Error); ok {
			if err3, ok := err2.Err.(net.Error); ok {
				if err3.Timeout() {
					cr.Lock()
					cr.timeout += 1
					cr.Unlock()
					return
				}
			}
		}
	}
	cr.Lock()
	if cr.other == 0 {
		cr.firstOther = &r
	}
	cr.other += 1
	cr.Unlock()
}

// checkMT queries all provided MT endpoints and provides a summary of all the outcomes;
// meaning the counts of each response matching each validator function, and the number
// of timeouts, and finally all others (non-timeouting invalid responses)
// we recommend for 60s duration to use 6000 requests, e.g. 100 per second
func CheckMT(endpoints []int, query, from string, dur time.Duration, reqs int, validators ...Validator) CheckResults {
	pre := time.Now()
	ret := newCheckResultsTemp(validators)
	period := dur / time.Duration(reqs)
	tick := time.NewTicker(period)
	issued := 0
	wg := &sync.WaitGroup{}
	for range tick.C {
		wg.Add(1)
		base := fmt.Sprintf("http://localhost:%d", endpoints[issued%len(endpoints)])
		req := RequestForLocalTesting(base, query, from)
		go checkWorker(req, wg, ret, validators)
		issued += 1
		if issued == reqs {
			break
		}
	}
	// note: could take 2 seconds longer than foreseen due to the client timeout, but anything longer may be a problem,
	wg.Wait()
	if time.Since(pre) > (110*dur/100)+2*time.Second {
		panic(fmt.Sprintf("checkMT ran too long for some reason. expected %s. took actually %s. system overloaded?", dur, time.Since(pre)))
	}
	return CheckResults{
		Valid:      ret.valid,
		Empty:      ret.empty,
		Timeout:    ret.timeout,
		Other:      ret.other,
		FirstOther: ret.firstOther,
	}
}
