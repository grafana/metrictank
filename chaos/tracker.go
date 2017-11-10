package chaos

import (
	"bufio"
	"fmt"
	"io"
	"os/exec"
	"regexp"
)

// Tracker allows to track stdout and stderr of running commands
// and wait for certain messages to appear
type Tracker struct {
	stdout        io.ReadCloser
	stderr        io.ReadCloser
	stdoutChan    chan string
	stderrChan    chan string
	errChan       chan error
	newMatcherCtx chan MatcherCtx
	logStdout     chan bool
	logStderr     chan bool
}

func NewTracker(cmd *exec.Cmd, logStdout, logStderr bool) (*Tracker, error) {
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
		make(chan MatcherCtx),
		make(chan bool),
		make(chan bool),
	}
	go t.track(t.stdout, t.stdoutChan)
	go t.track(t.stderr, t.stderrChan)
	go t.manage(logStdout, logStderr)
	return t, nil
}

func (t *Tracker) track(in io.ReadCloser, out chan string) {
	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		out <- scanner.Text()
	}
	if err := scanner.Err(); err != nil {
		t.errChan <- err
	}
}

func (t *Tracker) manage(logStdout, logStderr bool) {
	var matcherCtx []MatcherCtx
	for {
		select {
		case t := <-t.logStdout:
			logStdout = t
		case t := <-t.logStderr:
			logStderr = t
		case m := <-t.newMatcherCtx:
			matcherCtx = append(matcherCtx, m)
		case str := <-t.stdoutChan:
			if logStdout {
				fmt.Println("stdout:", str)
			}
			var tmp []MatcherCtx
			for _, m := range matcherCtx {
				if !m.Match(str, false) {
					tmp = append(tmp, m)
				}
			}
			matcherCtx = tmp
		case str := <-t.stderrChan:
			if logStderr {
				fmt.Println("stderr:", str)
			}
			var tmp []MatcherCtx
			for _, m := range matcherCtx {
				if !m.Match(str, true) {
					tmp = append(tmp, m)
				}
			}
			matcherCtx = tmp
		case err := <-t.errChan:
			panic(err)
		}
	}
}

type Matcher struct {
	Str    string
	Stderr bool
	r      *regexp.Regexp
	match  bool
}

type MatcherCtx struct {
	matchers []Matcher
	done     chan struct{}
}

// returns true when all matchers matched
func (m *MatcherCtx) Match(str string, stderr bool) bool {
	allMatch := true
	for i, matcher := range m.matchers {
		if matcher.Stderr != stderr {
			// if matcher is for stderr but str is stdout (or vice versa), don't try to match
			if !matcher.match {
				allMatch = false
			}
		} else if matcher.match {
			// matcher is for same fd but already matched previously
		} else if matcher.r.MatchString(str) {
			// matcher is for same fd (good), so try to match
			m.matchers[i].match = true
		} else {
			// no match
			allMatch = false
		}
	}
	if allMatch {
		close(m.done)
	}
	return allMatch
}

// Match returns a channel that will be closed when all matchers have matched
func (t *Tracker) Match(matchers []Matcher) chan struct{} {
	for i, m := range matchers {
		matchers[i].r = regexp.MustCompile(m.Str)
	}
	c := MatcherCtx{
		matchers: matchers,
		done:     make(chan struct{}),
	}
	t.newMatcherCtx <- c
	return c.done
}

func (t *Tracker) LogStdout(b bool) {
	t.logStdout <- b
}
func (t *Tracker) LogStderr(b bool) {
	t.logStderr <- b
}
