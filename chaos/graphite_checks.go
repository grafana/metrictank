package chaos

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/davecgh/go-spew/spew"
)

var renderClient *http.Client

func init() {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // only needed for graphite, not for MT. oh well..
	}
	renderClient = &http.Client{Transport: tr}
}

func renderQuery(base, target, from string) Response {
	url := fmt.Sprintf("%s/render?target=%s&format=json&from=%s", base, target, from)
	fmt.Println(url)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		panic(err)
	}
	req.Header.Add("X-Org-Id", "1") // only really needed for MT, not for graphite. oh well...
	resp, err := renderClient.Do(req)
	if err != nil {
		panic(err)
	}
	var response Response
	json.NewDecoder(resp.Body).Decode(&response)
	resp.Body.Close()
	return response
}

func retryGraphite(query, from string, times int, validate func(resp Response) bool) bool {
	return retry(query, from, times, validate, "https://localhost:443")
}
func retryMT(query, from string, times int, validate func(resp Response) bool) bool {
	return retry(query, from, times, validate, "http://localhost:6060")
}
func retry(query, from string, times int, validate func(resp Response) bool, base string) bool {
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

func validateTargets(resp Response, targets []string) bool {
	if len(resp) != len(targets) {
		return false
	}
	for i, r := range resp {
		if r.Target != targets[i] {
			return false
		}
	}
	return true
}
