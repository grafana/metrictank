package chaos

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
)

var renderClient *http.Client

func init() {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // only needed for graphite, not for MT. oh well..
	}
	renderClient = &http.Client{
		Transport: tr,
		Timeout:   time.Second * 2, // definitely works as timeout in waiting for response, not sure re dial timeout
	}
}

func renderQuery(base, target, from string) response {
	var r response
	url := fmt.Sprintf("%s/render?target=%s&format=json&from=%s", base, target, from)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	req.Header.Add("X-Org-Id", "1") // only really needed for MT, not for graphite. oh well...
	//fmt.Println("requesting", url)
	resp, err := renderClient.Do(req)
	if err != nil {
		r.httpErr = err
		return r
	}
	r.code = resp.StatusCode
	r.decodeErr = json.NewDecoder(resp.Body).Decode(&r.r)
	resp.Body.Close()
	return r
}

func retryGraphite(query, from string, times int, validate Validator) bool {
	return retry(query, from, times, validate, "https://localhost:443")
}
func retryMT(query, from string, times int, validate Validator) bool {
	return retry(query, from, times, validate, "http://localhost:6060")
}
func retry(query, from string, times int, validate Validator, base string) bool {
	for i := 0; i < times; i++ {
		if i > 0 {
			time.Sleep(time.Second)
		}
		resp := renderQuery(base, query, from)
		spew.Dump(resp)
		if validate(resp) {
			fmt.Println("VALID")
			return true
		}
		fmt.Println("INVALID")
	}
	return false
}

type checkResults struct {
	sync.Mutex
	valid   []int // each position corresponds to a validator
	invalid int
	timeout int
}

func newCheckResults(validators []Validator) *checkResults {
	return &checkResults{
		valid: make([]int, len(validators)),
	}
}

func checkWorker(base, query, from string, wg *sync.WaitGroup, cr *checkResults, validators []Validator) {
	r := renderQuery(base, query, from)
	found := false
	for i, v := range validators {
		if v(r) {
			found = true
			cr.Lock()
			cr.valid[i] += 1
			cr.Unlock()
		}
	}
	if r.httpErr != nil {
		if err2, ok := r.httpErr.(*url.Error); ok {
			if err3, ok := err2.Err.(net.Error); ok {
				if err3.Timeout() {
					found = true
					cr.Lock()
					cr.timeout += 1
					cr.Unlock()
				}
			}
		}
	}
	if !found {
		cr.Lock()
		cr.invalid += 1
		cr.Unlock()
	}
	wg.Done()
}

// checkMT queries all provided MT endpoints and provides a summary of all the outcomes;
// meaning the counts of each response matching each validator function, and the number
// of timeouts, and finally all others (non-timeouting invalid responses)
// we recommend for 60s duration to use 6000 requests, e.g. 100 per second
func checkMT(endpoints []int, query, from string, dur time.Duration, reqs int, validators ...Validator) checkResults {
	pre := time.Now()
	ret := newCheckResults(validators)
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
		panic(fmt.Sprintf("checkMT ran too long for some reason. expected %s. took actually %s. system overloaded?", dur, time.Since(pre)))
	}
	return *ret
}
