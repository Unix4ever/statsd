package statsd

import (
	"bytes"
	"log"
	"net"
	"time"
)

// StatsdSink provides a MetricSink that can be used
// with a statsite or statsd metrics server. It uses
// only UDP packets, while StatsiteSink uses TCP.
type StatsdSink struct {
	addr              string
	metricQueue       chan string
	statsdMaxLen      int
	flushInterval     time.Duration
	reconnectInterval time.Duration
}

// NewStatsdSink is used to create a new StatsdSink
func NewStatsdSink(addr string, statsdMaxLen int, flushInterval time.Duration,
	reconnectInterval time.Duration) *StatsdSink {
	s := &StatsdSink{
		addr:              addr,
		metricQueue:       make(chan string, 4096),
		statsdMaxLen:      statsdMaxLen,
		flushInterval:     flushInterval,
		reconnectInterval: reconnectInterval,
	}
	go s.flushMetrics()
	return s
}

// Close is used to stop flushing to statsd
func (s *StatsdSink) Shutdown() {
	close(s.metricQueue)
}

// Does a non-blocking push to the metrics queue
func (s *StatsdSink) PushMetric(m string) {
	select {
	case s.metricQueue <- m:
	default:
	}
}

// Flushes metrics
func (s *StatsdSink) flushMetrics() {
	var sock net.Conn
	var err error
	var wait <-chan time.Time
	ticker := time.NewTicker(s.flushInterval)
	reconnectTicker := time.NewTicker(s.reconnectInterval)
	defer ticker.Stop()
	defer reconnectTicker.Stop()

CONNECT:
	// Create a buffer
	buf := bytes.NewBuffer(nil)

RECONNECT:
	// Attempt to connect
	sock, err = net.Dial("udp", s.addr)
	if err != nil {
		log.Printf("[ERR] Error connecting to statsd! Err: %s", err)
		goto WAIT
	}

	for {
		select {
		case metric, ok := <-s.metricQueue:
			// Get a metric from the queue
			if !ok {
				goto QUIT
			}

			// Check if this would overflow the packet size
			if len(metric)+buf.Len() > s.statsdMaxLen {
				_, err := sock.Write(buf.Bytes())
				buf.Reset()
				if err != nil {
					log.Printf("[ERR] Error writing to statsd! Err: %s", err)
					goto WAIT
				}
			}

			if buf.Len() > 0 {
				buf.WriteByte('\n')
			}

			// Append to the buffer
			buf.WriteString(metric)
		case <-ticker.C:
			if buf.Len() == 0 {
				continue
			}

			_, err := sock.Write(buf.Bytes())
			buf.Reset()
			if err != nil {
				log.Printf("[ERR] Error flushing to statsd! Err: %s", err)
				goto WAIT
			}
		case <-reconnectTicker.C:
			log.Printf("Reconnecting to statsd")
			goto RECONNECT
		}
	}

WAIT:
	// Wait for a while
	wait = time.After(time.Duration(5) * time.Second)
	for {
		select {
		// Dequeue the messages to avoid backlog
		case _, ok := <-s.metricQueue:
			if !ok {
				goto QUIT
			}
		case <-wait:
			goto CONNECT
		}
	}
QUIT:
	s.metricQueue = nil
}
