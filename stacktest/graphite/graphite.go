package graphite

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

var renderClient *http.Client

func init() {
	renderClient = &http.Client{
		// definitely works as timeout in waiting for response, not sure re dial timeout
		// note : MT's internal cross-cluster timeouts are 5s, so we have to wait at least as long to classify properly
		Timeout: time.Second * 6,
	}
}

func renderQuery(base, target, from string) Response {
	var r Response
	url := fmt.Sprintf("%s/render?target=%s&format=json&from=%s", base, target, from)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Panic("request failed")
	}
	req.Header.Add("X-Org-Id", "1") // only really needed for MT, not for graphite. oh well...
	//fmt.Println("requesting", url)
	resp, err := renderClient.Do(req)
	if err != nil {
		r.httpErr = err
		return r
	}
	r.code = resp.StatusCode
	traceHeader := resp.Header["Trace-Id"]
	if len(traceHeader) > 0 {
		r.traceID = traceHeader[0]
	}
	r.decodeErr = json.NewDecoder(resp.Body).Decode(&r.r)
	resp.Body.Close()
	return r
}

func RetryGraphite(query, from string, times int, validate Validator) (bool, Response) {
	return retry(query, from, times, validate, "http://localhost")
}

func RetryGraphite8080(query, from string, times int, validate Validator) (bool, Response) {
	return retry(query, from, times, validate, "http://localhost:8080")
}

func RetryMT(query, from string, times int, validate Validator) (bool, Response) {
	return retry(query, from, times, validate, "http://localhost:6060")
}
func retry(query, from string, times int, validate Validator, base string) (bool, Response) {
	var resp Response
	for i := 0; i < times; i++ {
		if i > 0 {
			time.Sleep(time.Second)
		}
		resp = renderQuery(base, query, from)
		if validate(resp) {
			return true, resp
		}
	}
	return false, resp
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

func checkWorker(base, query, from string, wg *sync.WaitGroup, cr *checkResultsTemp, validators []Validator) {
	r := renderQuery(base, query, from)
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
	if r.httpErr == nil && r.decodeErr == nil && len(r.r) == 0 {
		cr.Lock()
		cr.empty += 1
		cr.Unlock()
		return
	}
	if r.httpErr != nil {
		if err2, ok := r.httpErr.(*url.Error); ok {
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
		go checkWorker(base, query, from, wg, ret, validators)
		issued += 1
		if issued == reqs {
			break
		}
	}
	// note: could take 2 seconds longer than foreseen due to the client timeout, but anything longer may be a problem,
	wg.Wait()
	if time.Since(pre) > (110*dur/100)+2*time.Second {
		log.WithFields(log.Fields{
			"expected.duration": dur,
			"actual.duration":   time.Since(pre),
		}).Panic("checkMT ran too long, system overloaded?")
	}
	return CheckResults{
		Valid:      ret.valid,
		Empty:      ret.empty,
		Timeout:    ret.timeout,
		Other:      ret.other,
		FirstOther: ret.firstOther,
	}
}
