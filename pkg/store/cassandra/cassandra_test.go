package cassandra

import (
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/grafana/metrictank/logger"
	log "github.com/sirupsen/logrus"
)

type testCase struct {
	ttl                uint32
	windowFactor       int
	nameFormat         string
	expectedTableName  string
	expectedWindowSize uint32
}

const oneSecond = 1
const oneMinute = 60
const oneHour = 60 * 60
const oneDay = 24 * 60 * 60
const oneMonth = oneDay * 30
const oneYear = oneMonth * 12

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func TestGetTTLTables(t *testing.T) {
	tcs := []testCase{
		// that's no real world scenario, but let's test it anyway
		{0, 20, "metric_%d", "metric_0", 1},

		{oneSecond, 20, "metric_%d", "metric_0", 1},
		{oneMinute, 20, "metric_%d", "metric_0", 1},
		{oneHour, 20, "metric_%d", "metric_1", 1},
		{oneDay, 20, "metric_%d", "metric_16", 1},
		{oneMonth, 20, "metric_%d", "metric_512", 26},
		{oneYear, 20, "metric_%d", "metric_8192", 410},

		// cases around the edge
		{1024*3600 - 1, 20, "metric_%d", "metric_512", 26},
		{1024 * 3600, 20, "metric_%d", "metric_1024", 52},

		// alternative window factor
		{oneDay, 50, "metric_%d", "metric_16", 1},
		{oneMonth, 50, "metric_%d", "metric_512", 11},
		{oneYear, 50, "metric_%d", "metric_8192", 164},

		// in python3: math.floor(math.pow(2, math.floor(math.log2(math.pow(2,32)/3600)))/100)+1 = 10486
		{math.MaxUint32, 100, "metric_%d", "metric_1048576", 10486},
	}

	for _, tc := range tcs {
		result := GetTTLTables([]uint32{tc.ttl}, tc.windowFactor, tc.nameFormat)
		logPrefix := fmt.Sprintf("TTL: %d, WF: %d: ", tc.ttl, tc.windowFactor)
		if len(result) != 1 {
			t.Fatalf("%s expected 1 result, got %d", logPrefix, len(result))
		}
		if result[tc.ttl].Name != tc.expectedTableName {
			t.Fatalf("%s expected table name %q, got %q", logPrefix, tc.expectedTableName, result[tc.ttl].Name)
		}
		if result[tc.ttl].WindowSize != tc.expectedWindowSize {
			t.Fatalf("%s expected window size %d, got %d", logPrefix, tc.expectedWindowSize, result[tc.ttl].WindowSize)
		}
	}
}

func TestBackwardsCompatibleTimeout(t *testing.T) {
	checkTimeout := func(input string, expected, defaultUnit time.Duration) {
		timeoutD := ConvertTimeout(input, defaultUnit)
		if timeoutD != expected {
			t.Fatalf("expected time %s but got %s from input %s", expected.String(), timeoutD.String(), input)
		}
	}

	checkTimeout("3500", time.Duration(3500)*time.Millisecond, time.Millisecond)
	checkTimeout("3500ms", time.Duration(3500)*time.Millisecond, time.Millisecond)
	checkTimeout("3.5s", time.Duration(3500)*time.Millisecond, time.Millisecond)
	checkTimeout("35", time.Duration(35)*time.Hour, time.Hour)
}

// copied from https://blog.antoine-augusti.fr/2015/12/testing-an-os-exit-scenario-in-golang/
func TestBackwardsCompatibleTimeoutFatalIfInvalid(t *testing.T) {
	if os.Getenv("CONVERT_INVALID_TIMEOUT") == "1" {
		ConvertTimeout("invalid", time.Millisecond)
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestBackwardsCompatibleTimeoutFatalIfInvalid")
	cmd.Env = append(os.Environ(), "CONVERT_INVALID_TIMEOUT=1")
	stdout, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	gotBytes, _ := ioutil.ReadAll(stdout)
	got := string(gotBytes)
	expected := "invalid duration value \"invalid\""
	if !strings.HasSuffix(got[:len(got)-1], expected) {
		t.Fatalf("Unexpected log message. Expected message to contain \"%s\" but got \"%s\"", expected, got)
	}

	err := cmd.Wait()
	if e, ok := err.(*exec.ExitError); !ok || e.Success() {
		t.Fatalf("Process ran with err %v, want exit status 1", err)
	}
}
