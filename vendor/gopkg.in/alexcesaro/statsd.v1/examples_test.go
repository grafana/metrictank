package statsd_test

import (
	"log"
	"net/http"
	"runtime"
	"time"

	"github.com/alexcesaro/statsd"
)

func Example() {
	c, err := statsd.New(":8125")
	if err != nil {
		panic(err)
	}

	c.Increment("foo.counter")
	c.Gauge("num_goroutine", runtime.NumGoroutine())

	t := c.NewTiming()
	http.Get("http://example.com/")
	t.Send("homepage.response_time", 1)
	// Can also be used as a one-liner in a function:
	// func PingHomepage() {
	// 	defer c.NewTiming().Send("homepage.response_time", 1)
	// 	http.Get("http://example.com/")
	// }

	c.Close()
}

func ExampleMute() {
	c, err := statsd.New(":8125", statsd.Mute(true))
	if err != nil {
		panic(err)
	}
	c.Increment("foo.bar") // Does nothing.
}

func ExampleWithDatadogTags() {
	statsd.New(":8125", statsd.WithDatadogTags("region:us", "app:my_app"))
}

func ExampleWithErrorHandler() {
	statsd.New(":8125", statsd.WithErrorHandler(func(err error) {
		log.Print(err)
	}))
}

func ExampleWithFlushPeriod() {
	statsd.New(":8125", statsd.WithFlushPeriod(10*time.Millisecond))
}

func ExampleWithInfluxDBTags() {
	statsd.New(":8125", statsd.WithInfluxDBTags("region", "us", "app", "my_app"))
}

func ExampleWithMaxPacketSize() {
	statsd.New(":8125", statsd.WithMaxPacketSize(512))
}

func ExampleWithNetwork() {
	// Send metrics using a TCP connection.
	statsd.New(":8125", statsd.WithNetwork("tcp"))
}

func ExampleWithPrefix() {
	c, err := statsd.New(":8125", statsd.WithPrefix("my_app."))
	if err != nil {
		panic(err)
	}
	c.Increment("foo.bar") // Increments "my_app.foo.bar".
}

var c *statsd.Client

func ExampleClient_NewTiming() {
	// Send a timing metric each time the function is run.
	defer c.NewTiming().Send("homepage.response_time", 1)
	http.Get("http://example.com/")
}
