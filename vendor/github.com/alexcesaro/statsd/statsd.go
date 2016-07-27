package statsd

import (
	"bytes"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

// A Client represents a StatsD client.
type Client struct {
	mu sync.Mutex

	// Fields guarded by the mutex.
	conn      net.Conn
	buf       []byte
	rateCache map[float32]string
	closed    bool

	// Fields settable with options at Client's creation.
	muted         bool
	errorHandler  func(error)
	flushPeriod   time.Duration
	maxPacketSize int
	network       string
	prefix        string
	tagFormat     tagFormat
	tags          string
}

// An Option represents an option for a Client. It must be used as an argument
// to New().
type Option func(*Client)

// Mute sets whether the Client is muted.
func Mute(b bool) Option {
	return Option(func(c *Client) {
		c.muted = b
	})
}

// WithErrorHandler sets the error handling function used by the Client.
func WithErrorHandler(h func(error)) Option {
	return Option(func(c *Client) {
		c.errorHandler = h
	})
}

// WithFlushPeriod sets how often the Client's buffer is flushed.
// If period is 0, the goroutine that periodically flush the buffer is not
// lauched and the buffer is only flushed when it is full.
//
// By default the flush period is 100 milliseconds.
func WithFlushPeriod(period time.Duration) Option {
	return Option(func(c *Client) {
		c.flushPeriod = period
	})
}

// WithMaxPacketSize sets the maximum packet size in bytes sent by the Client.
//
// By default it is 1440.
func WithMaxPacketSize(n int) Option {
	return Option(func(c *Client) {
		c.maxPacketSize = n
	})
}

// WithNetwork sets the network (udp, tcp, etc) used by the client.
// See net.Dial documentation: https://golang.org/pkg/net/#Dial
//
// By default, network is udp.
func WithNetwork(network string) Option {
	return Option(func(c *Client) {
		c.network = network
	})
}

// WithPrefix sets the prefix prepended to every bucket name.
func WithPrefix(prefix string) Option {
	return Option(func(c *Client) {
		c.prefix = prefix
	})
}

// WithDatadogTags sets the Datadog tags sent with every metrics.
//
// The tags should have the key:value syntax.
// See http://docs.datadoghq.com/guides/metrics/#tags
func WithDatadogTags(tags ...string) Option {
	return Option(func(c *Client) {
		// Datadog tag format: |#tag1:value1,tag2,tag3:value3
		// See http://docs.datadoghq.com/guides/dogstatsd/#datagram-format
		buf := bytes.NewBufferString("|#")
		first := true
		for i := 0; i < len(tags); i++ {
			if first {
				first = false
			} else {
				buf.WriteByte(',')
			}
			buf.WriteString(tags[i])
		}
		c.tagFormat = datadogFormat
		c.tags = buf.String()
	})
}

// WithInfluxDBTags sets the InfluxDB tags sent with every metrics.
//
// The tags must be set as key-value pairs. If the number of tags is not even,
// WithInfluxDBTags panics.
//
// See https://influxdb.com/blog/2015/11/03/getting_started_with_influx_statsd.html
func WithInfluxDBTags(tags ...string) Option {
	if len(tags)%2 != 0 {
		panic("statsd: WithInfluxDBTags only accepts an even number arguments")
	}

	// InfluxDB tag format: ,tag1=payroll,region=us-west
	// https://influxdb.com/blog/2015/11/03/getting_started_with_influx_statsd.html
	return Option(func(c *Client) {
		var buf bytes.Buffer
		for i := 0; i < len(tags)/2; i++ {
			buf.WriteByte(',')
			buf.WriteString(tags[2*i])
			buf.WriteByte('=')
			buf.WriteString(tags[2*i+1])
		}
		c.tagFormat = influxDBFormat
		c.tags = buf.String()
	})
}

type tagFormat uint8

const (
	datadogFormat tagFormat = iota + 1
	influxDBFormat
)

// New creates a new Client with the given options.
func New(addr string, options ...Option) (*Client, error) {
	c := &Client{
		// Worst-case scenario:
		// Ethernet MTU - IPv6 Header - TCP Header = 1500 - 40 - 20 = 1440
		maxPacketSize: 1440,
	}

	for _, o := range options {
		o(c)
	}

	if c.muted {
		return c, nil
	}

	if c.network == "" {
		c.network = "udp"
	}
	var err error
	c.conn, err = dialTimeout(c.network, addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	// When using UDP do a quick check to see if something is listening on the
	// given port to return an error as soon as possible.
	if c.network[:3] == "udp" {
		for i := 0; i < 2; i++ {
			_, err = c.conn.Write(nil)
			if err != nil {
				_ = c.conn.Close()
				return nil, err
			}
		}
	}

	if c.flushPeriod == 0 {
		c.flushPeriod = 100 * time.Millisecond
	}

	// To prevent a buffer overflow add some capacity to the buffer to allow for
	// an additional metric.
	c.buf = make([]byte, 0, c.maxPacketSize+200)

	if c.flushPeriod > 0 {
		go func() {
			ticker := time.NewTicker(c.flushPeriod)
			for _ = range ticker.C {
				c.mu.Lock()
				if c.closed {
					ticker.Stop()
					c.mu.Unlock()
					return
				}
				c.flush(0)
				c.mu.Unlock()
			}
		}()
	}

	return c, nil
}

// Count adds n to bucket with the given sampling rate.
func (c *Client) Count(bucket string, n int, rate float32) {
	if c.muted {
		return
	}
	if isRandAbove(rate) {
		return
	}

	c.mu.Lock()
	l := len(c.buf)
	c.appendBucket(bucket)
	c.appendInt(n)
	c.appendType("c")
	c.appendRate(rate)
	c.closeMetric()
	c.flushIfBufferFull(l)
	c.mu.Unlock()
}

func isRandAbove(rate float32) bool {
	return rate != 1 && randFloat() > rate
}

// Increment increment the given bucket.
// It is equivalent to Count(bucket, 1, 1).
func (c *Client) Increment(bucket string) {
	c.Count(bucket, 1, 1)
}

// Gauge records an absolute value for the given bucket.
func (c *Client) Gauge(bucket string, value int) {
	if c.muted {
		return
	}

	c.mu.Lock()
	l := len(c.buf)
	// To set a gauge to a negative value we must first set it to 0.
	// https://github.com/etsy/statsd/blob/master/docs/metric_types.md#gauges
	if value < 0 {
		c.appendBucket(bucket)
		c.gauge(0)
	}
	c.appendBucket(bucket)
	c.gauge(value)
	c.flushIfBufferFull(l)
	c.mu.Unlock()
}

// ChangeGauge changes the value of a gauge by the given delta.
func (c *Client) ChangeGauge(bucket string, delta int) {
	if c.muted {
		return
	}
	if delta == 0 {
		return
	}

	c.mu.Lock()
	l := len(c.buf)
	c.appendBucket(bucket)
	if delta > 0 {
		c.appendByte('+')
	}
	c.gauge(delta)
	c.flushIfBufferFull(l)
	c.mu.Unlock()
}

func (c *Client) gauge(value int) {
	c.appendInt(value)
	c.appendType("g")
	c.closeMetric()
}

// Timing sends a timing value to a bucket with the given sampling rate.
func (c *Client) Timing(bucket string, value int, rate float32) {
	if c.muted {
		return
	}
	if isRandAbove(rate) {
		return
	}

	c.mu.Lock()
	l := len(c.buf)
	c.appendBucket(bucket)
	c.appendInt(value)
	c.appendType("ms")
	c.appendRate(rate)
	c.closeMetric()
	c.flushIfBufferFull(l)
	c.mu.Unlock()
}

// A Timing is an helper object that eases sending timing values.
type Timing struct {
	start time.Time
	c     *Client
}

// NewTiming creates a new Timing.
func (c *Client) NewTiming() Timing {
	return Timing{start: now(), c: c}
}

// Send sends the time elapsed since the creation of the Timing to a bucket
// with the given sampling rate.
func (t Timing) Send(bucket string, rate float32) {
	t.c.Timing(bucket, int(t.Duration()/time.Millisecond), rate)
}

// Duration gets the duration since the creation of the Timing.
func (t Timing) Duration() time.Duration {
	return now().Sub(t.start)
}

// Unique sends the given value to a set bucket.
func (c *Client) Unique(bucket string, value string) {
	if c.muted {
		return
	}

	c.mu.Lock()
	l := len(c.buf)
	c.appendBucket(bucket)
	c.appendString(value)
	c.appendType("s")
	c.closeMetric()
	c.flushIfBufferFull(l)
	c.mu.Unlock()
}

// Flush flushes the Client's buffer.
func (c *Client) Flush() {
	if c.muted {
		return
	}

	c.mu.Lock()
	c.flush(0)
	c.mu.Unlock()
}

// Close flushes the Client's buffer and releases the associated ressources.
func (c *Client) Close() {
	if c.muted {
		return
	}

	c.mu.Lock()
	c.flush(0)
	c.handleError(c.conn.Close())
	c.closed = true
	c.mu.Unlock()
}

func (c *Client) appendByte(b byte) {
	c.buf = append(c.buf, b)
}

func (c *Client) appendString(s string) {
	c.buf = append(c.buf, s...)
}

func (c *Client) appendInt(i int) {
	c.buf = strconv.AppendInt(c.buf, int64(i), 10)
}

func (c *Client) appendBucket(bucket string) {
	if c.prefix != "" {
		c.appendString(c.prefix)
	}
	c.appendString(bucket)
	if c.tagFormat == influxDBFormat {
		c.appendString(c.tags)
	}
	c.appendByte(':')
}

func (c *Client) appendType(t string) {
	c.appendByte('|')
	c.appendString(t)
}

func (c *Client) appendRate(rate float32) {
	if rate == 1 {
		return
	}
	if c.rateCache == nil {
		c.rateCache = make(map[float32]string)
	}

	c.appendString("|@")
	if s, ok := c.rateCache[rate]; ok {
		c.appendString(s)
	} else {
		s = strconv.FormatFloat(float64(rate), 'f', -1, 32)
		c.rateCache[rate] = s
		c.appendString(s)
	}
}

func (c *Client) closeMetric() {
	if c.tagFormat == datadogFormat {
		c.appendString(c.tags)
	}
	c.appendByte('\n')
}

func (c *Client) flushIfBufferFull(lastSafeLen int) {
	if len(c.buf) > c.maxPacketSize {
		c.flush(lastSafeLen)
	}
}

// flush flushes the first n bytes of the buffer.
// If n is 0, the whole buffer is flushed.
func (c *Client) flush(n int) {
	if len(c.buf) == 0 {
		return
	}
	if n == 0 {
		n = len(c.buf)
	}

	// Trim the last \n, StatsD does not like it.
	_, err := c.conn.Write(c.buf[:n-1])
	c.handleError(err)
	if n < len(c.buf) {
		copy(c.buf, c.buf[n:])
	}
	c.buf = c.buf[:len(c.buf)-n]
}

func (c *Client) handleError(err error) {
	if err != nil && c.errorHandler != nil {
		c.errorHandler(err)
	}
}

// Stubbed out for testing.
var (
	dialTimeout = net.DialTimeout
	now         = time.Now
	randFloat   = rand.Float32
)
