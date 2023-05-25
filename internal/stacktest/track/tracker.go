package track

import (
	"bufio"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"sync"
)

// Tracker allows to track stdout and stderr of running commands
// and wait for certain messages to appear
type Tracker struct {
	stdout        io.ReadCloser
	stderr        io.ReadCloser
	stdoutChan    chan string
	stderrChan    chan string
	errChan       chan error
	newMatcherSet chan MatcherSet
	logStdout     chan bool
	logStderr     chan bool
	prefixStdout  string
	prefixStderr  string
	wg            sync.WaitGroup
}

func NewTracker(cmd *exec.Cmd, logStdout, logStderr bool, prefixStdout, prefixStderr string) (*Tracker, error) {
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	t := &Tracker{
		stdout,
		stderr,
		make(chan string),
		make(chan string),
		make(chan error),
		make(chan MatcherSet),
		make(chan bool),
		make(chan bool),
		prefixStdout,
		prefixStderr,
		sync.WaitGroup{},
	}
	if prefixStdout == "" {
		t.prefixStdout = "stdout:"
	}
	if prefixStderr == "" {
		t.prefixStderr = "stderr:"
	}
	go t.track(t.stdout, t.stdoutChan)
	go t.track(t.stderr, t.stderrChan)
	t.wg.Add(1)
	go t.manage(logStdout, logStderr)
	return t, nil
}

// track sends every line read from `in` to the channel `out`.
// if an error is encountered, it goes to `errChan` and `out` is closed.
func (t *Tracker) track(in io.ReadCloser, out chan string) {
	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		read := scanner.Text()
		out <- read
	}
	err := scanner.Err()
	if err != nil {
		t.errChan <- err
	}
	close(out)
}

func (t *Tracker) manage(logStdout, logStderr bool) {
	var doneStdout bool
	var doneStderr bool

	// all matcherSets that are not wholely matched yet
	var matcherSet []MatcherSet

	for {
		select {
		case t := <-t.logStdout:
			logStdout = t
		case t := <-t.logStderr:
			logStderr = t
		case m := <-t.newMatcherSet:
			matcherSet = append(matcherSet, m)
		case str, ok := <-t.stdoutChan:
			if !ok {
				doneStdout = true
				break
			}
			if logStdout {
				fmt.Println(t.prefixStdout, str)
			}
			var tmp []MatcherSet
			for _, m := range matcherSet {
				if !m.Match(str, false) {
					tmp = append(tmp, m)
				}
			}
			matcherSet = tmp
		case str, ok := <-t.stderrChan:
			if !ok {
				doneStderr = true
				break
			}
			if logStderr {
				fmt.Println(t.prefixStderr, str)
			}
			var tmp []MatcherSet
			for _, m := range matcherSet {
				if !m.Match(str, true) {
					tmp = append(tmp, m)
				}
			}
			matcherSet = tmp
		case err := <-t.errChan:
			panic(err)
		}
		if doneStdout && doneStderr {
			t.wg.Done()
			return
		}
	}
}

// Matcher describes the matching of a given string on a certain stream (stdout or stderr)
type Matcher struct {
	Str    string
	Stderr bool
	r      *regexp.Regexp
	match  bool
}

// MatcherSet encapsulates a set of matchers
// when all of them match, `done` will be closed.
type MatcherSet struct {
	matchers []Matcher
	verbose  bool
	done     chan struct{}
}

func NewMatcherSet(matchers []Matcher, verbose bool) MatcherSet {
	for i, m := range matchers {
		matchers[i].r = regexp.MustCompile(m.Str)
	}
	return MatcherSet{
		matchers: matchers,
		verbose:  verbose,
		done:     make(chan struct{}),
	}
}

// Match returns whether all matchers either:
// * currently match the string and stream type (stdout/stderr)
// * have matched previously
// and closes the `done` channel if so.
func (m *MatcherSet) Match(str string, stderr bool) bool {
	allMatch := true
	for i, matcher := range m.matchers {

		// check whether the string's stream (stdout/stderr) even applies to the matcher
		// if not, skip the matching, but honor previous outcome, if any.
		if matcher.Stderr != stderr {
			if !matcher.match {
				allMatch = false
			}
			continue
		}

		// if already matched previously, nothing left to do
		if matcher.match {
			continue
		}

		if matcher.r.MatchString(str) {
			if m.verbose {
				fmt.Println("Matcher matched:", matcher.Str)
			}
			m.matchers[i].match = true
		} else {
			allMatch = false
		}
	}
	if allMatch {
		close(m.done)
	}
	return allMatch
}

// Match creates a MatcherSet, registers it and returns a channel that will be closed when all matchers have matched
func (t *Tracker) Match(matchers []Matcher, verbose bool) chan struct{} {
	m := NewMatcherSet(matchers, verbose)
	t.newMatcherSet <- m
	return m.done
}

func (t *Tracker) LogStdout(b bool) {
	t.logStdout <- b
}
func (t *Tracker) LogStderr(b bool) {
	t.logStderr <- b
}

// Wait waits until stdout and stdin are closed
func (t *Tracker) Wait() {
	t.wg.Wait()
}
