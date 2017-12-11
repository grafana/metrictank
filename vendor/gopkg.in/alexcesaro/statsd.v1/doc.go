/*
Package statsd is a simple and efficient StatsD client.

Client's methods are fast and do not allocate memory.

Internally, Client's methods buffers metrics. The buffer is flushed when either:
 - the background goroutine flushes the buffer (every 100ms by default)
 - the buffer is full (1440 bytes by default so that IP packets are not
   fragmented)

The background goroutine can be disabled using the WithFlushPeriod(0) option.

Buffering can be disabled using the WithMaxPacketSize(0) option.

StatsD homepage: https://github.com/etsy/statsd
*/
package statsd
