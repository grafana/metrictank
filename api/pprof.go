package api

import (
	"net/http"
	"runtime"
	rpprof "runtime/pprof"
	"strconv"
	"time"
)

// blockhandler writes out a blocking profile
// similar to the standard library handler,
// except it allows to specify a rate.
// The profiler aims to sample an average of one blocking event
// per rate nanoseconds spent blocked.
//
// To include every blocking event in the profile, pass rate = 1.
// Defaults to 10k (10 microseconds)
func blockHandler(w http.ResponseWriter, r *http.Request) {
	debug, _ := strconv.Atoi(r.FormValue("debug"))
	sec, _ := strconv.ParseInt(r.FormValue("seconds"), 10, 64)
	if sec == 0 {
		sec = 30
	}
	rate, _ := strconv.Atoi(r.FormValue("rate"))
	if rate == 0 {
		rate = 10000
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	runtime.SetBlockProfileRate(rate)
	time.Sleep(time.Duration(sec) * time.Second)
	runtime.SetBlockProfileRate(0)
	p := rpprof.Lookup("block")
	p.WriteTo(w, debug)
}

// mutexHandler writes out a mutex profile similar to the
// standard library handler,
// except it allows to specify a mutex profiling rate.
// On average 1/rate events are reported.  The default is 1000
func mutexHandler(w http.ResponseWriter, r *http.Request) {
	debug, _ := strconv.Atoi(r.FormValue("debug"))
	sec, _ := strconv.ParseInt(r.FormValue("seconds"), 10, 64)
	if sec == 0 {
		sec = 30
	}
	rate, _ := strconv.Atoi(r.FormValue("rate"))
	if rate == 0 {
		rate = 1000
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	runtime.SetMutexProfileFraction(rate)
	time.Sleep(time.Duration(sec) * time.Second)
	runtime.SetMutexProfileFraction(0)
	p := rpprof.Lookup("mutex")
	p.WriteTo(w, debug)
}
