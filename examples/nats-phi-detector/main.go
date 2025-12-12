// Copyright 2012-2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Phi Accrual Failure Detector Example
//
// This example demonstrates implementing the Phi Accrual Failure Detector
// algorithm using NATS subscriptions for heartbeat monitoring.
//
// The Phi Accrual Failure Detector (Hayashibara et al.) provides a flexible
// failure detection mechanism that outputs a continuous "phi" value representing
// the suspicion level that a monitored node has failed. Unlike binary failure
// detectors, this approach allows applications to choose their own threshold
// based on their specific requirements.
//
// Usage:
//   # Terminal 1 - Start the monitor
//   go run main.go -s nats://localhost:4222 -mode monitor -node mynode
//
//   # Terminal 2 - Start the heartbeat sender
//   go run main.go -s nats://localhost:4222 -mode heartbeat -node mynode
//
// The monitor will display phi values as heartbeats arrive. When the heartbeat
// sender is stopped, phi values will increase, indicating suspected failure.

package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	// DefaultPhiThreshold is the default threshold above which a node is
	// considered failed. A phi of 8 corresponds to roughly 99.9997% probability
	// that the node has failed.
	DefaultPhiThreshold = 8.0

	// DefaultHeartbeatInterval is the default interval between heartbeats.
	DefaultHeartbeatInterval = 1 * time.Second

	// DefaultWindowSize is the number of heartbeat intervals to keep for
	// statistical calculations.
	DefaultWindowSize = 100

	// MinStdDeviation is the minimum standard deviation to use, preventing
	// division by zero and overly sensitive detection.
	MinStdDeviation = 100 * time.Millisecond
)

// PhiAccrualDetector implements the Phi Accrual Failure Detector algorithm.
// It maintains a sliding window of heartbeat inter-arrival times and uses
// them to calculate the phi (φ) value that represents the suspicion level.
type PhiAccrualDetector struct {
	mu sync.RWMutex

	// Sliding window of inter-arrival times
	intervals    []time.Duration
	windowSize   int
	intervalIdx  int
	intervalsFull bool

	// Timestamp of last heartbeat
	lastHeartbeat time.Time

	// Phi threshold for failure detection
	threshold float64

	// Minimum standard deviation to prevent instability
	minStdDev time.Duration
}

// NewPhiAccrualDetector creates a new Phi Accrual Failure Detector with the
// specified configuration.
func NewPhiAccrualDetector(windowSize int, threshold float64, minStdDev time.Duration) *PhiAccrualDetector {
	return &PhiAccrualDetector{
		intervals:   make([]time.Duration, windowSize),
		windowSize:  windowSize,
		threshold:   threshold,
		minStdDev:   minStdDev,
	}
}

// Heartbeat records a heartbeat arrival. This should be called each time
// a heartbeat message is received from the monitored node.
func (d *PhiAccrualDetector) Heartbeat() {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	if !d.lastHeartbeat.IsZero() {
		interval := now.Sub(d.lastHeartbeat)
		d.intervals[d.intervalIdx] = interval
		d.intervalIdx = (d.intervalIdx + 1) % d.windowSize
		if d.intervalIdx == 0 {
			d.intervalsFull = true
		}
	}
	d.lastHeartbeat = now
}

// Phi calculates the current phi value. Higher values indicate higher
// suspicion that the monitored node has failed.
//
// The phi value is calculated as: φ = -log10(1 - F(timeSinceLastHeartbeat))
// where F is the CDF of a normal distribution based on observed intervals.
func (d *PhiAccrualDetector) Phi() float64 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.lastHeartbeat.IsZero() {
		return 0.0 // No heartbeats yet
	}

	count := d.count()
	if count < 2 {
		return 0.0 // Not enough data
	}

	timeSinceLast := time.Since(d.lastHeartbeat)
	mean, stdDev := d.stats(count)

	// Ensure minimum standard deviation
	if stdDev < d.minStdDev {
		stdDev = d.minStdDev
	}

	return phi(timeSinceLast, mean, stdDev)
}

// IsAvailable returns true if the monitored node is considered available
// (phi is below the threshold).
func (d *PhiAccrualDetector) IsAvailable() bool {
	return d.Phi() < d.threshold
}

// Stats returns the current mean and standard deviation of inter-arrival times.
func (d *PhiAccrualDetector) Stats() (mean, stdDev time.Duration) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	count := d.count()
	if count < 2 {
		return 0, 0
	}

	m, s := d.stats(count)
	return m, s
}

// TimeSinceLastHeartbeat returns the duration since the last heartbeat.
func (d *PhiAccrualDetector) TimeSinceLastHeartbeat() time.Duration {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.lastHeartbeat.IsZero() {
		return 0
	}
	return time.Since(d.lastHeartbeat)
}

// count returns the number of recorded intervals.
func (d *PhiAccrualDetector) count() int {
	if d.intervalsFull {
		return d.windowSize
	}
	return d.intervalIdx
}

// stats calculates mean and standard deviation from the recorded intervals.
func (d *PhiAccrualDetector) stats(count int) (mean, stdDev time.Duration) {
	if count == 0 {
		return 0, 0
	}

	// Calculate mean
	var sum time.Duration
	for i := 0; i < count; i++ {
		sum += d.intervals[i]
	}
	mean = sum / time.Duration(count)

	// Calculate standard deviation
	if count < 2 {
		return mean, 0
	}

	var variance float64
	for i := 0; i < count; i++ {
		diff := float64(d.intervals[i] - mean)
		variance += diff * diff
	}
	variance /= float64(count - 1)
	stdDev = time.Duration(math.Sqrt(variance))

	return mean, stdDev
}

// phi calculates the phi value using the normal distribution CDF.
// φ = -log10(1 - CDF(t)) where t is the time since last heartbeat.
func phi(timeSinceLast, mean, stdDev time.Duration) float64 {
	// Calculate the probability that we should have received a heartbeat by now
	// using the CDF of a normal distribution
	y := float64(timeSinceLast-mean) / float64(stdDev)

	// Use the error function to calculate the CDF
	// CDF(x) = 0.5 * (1 + erf(x / sqrt(2)))
	p := 0.5 * (1.0 + erf(y/math.Sqrt2))

	// Clamp p to avoid log of 0 or negative numbers
	if p >= 1.0 {
		p = 1.0 - 1e-15
	}
	if p <= 0.0 {
		return 0.0
	}

	// φ = -log10(1 - p)
	return -math.Log10(1.0 - p)
}

// erf approximates the error function using Horner's method.
// This is a polynomial approximation accurate to about 1.5e-7.
func erf(x float64) float64 {
	// Constants for the approximation
	a1 := 0.254829592
	a2 := -0.284496736
	a3 := 1.421413741
	a4 := -1.453152027
	a5 := 1.061405429
	p := 0.3275911

	// Save the sign
	sign := 1.0
	if x < 0 {
		sign = -1.0
		x = -x
	}

	// Approximation
	t := 1.0 / (1.0 + p*x)
	y := 1.0 - (((((a5*t+a4)*t)+a3)*t+a2)*t+a1)*t*math.Exp(-x*x)

	return sign * y
}

// NodeMonitor monitors multiple nodes using phi accrual failure detection.
type NodeMonitor struct {
	mu        sync.RWMutex
	detectors map[string]*PhiAccrualDetector
	nc        *nats.Conn
	threshold float64
	windowSize int
}

// NewNodeMonitor creates a new node monitor.
func NewNodeMonitor(nc *nats.Conn, threshold float64, windowSize int) *NodeMonitor {
	return &NodeMonitor{
		detectors:  make(map[string]*PhiAccrualDetector),
		nc:         nc,
		threshold:  threshold,
		windowSize: windowSize,
	}
}

// GetOrCreateDetector returns the detector for a node, creating it if needed.
func (m *NodeMonitor) GetOrCreateDetector(nodeID string) *PhiAccrualDetector {
	m.mu.Lock()
	defer m.mu.Unlock()

	if d, ok := m.detectors[nodeID]; ok {
		return d
	}

	d := NewPhiAccrualDetector(m.windowSize, m.threshold, MinStdDeviation)
	m.detectors[nodeID] = d
	return d
}

// GetDetector returns the detector for a node, or nil if not found.
func (m *NodeMonitor) GetDetector(nodeID string) *PhiAccrualDetector {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.detectors[nodeID]
}

// AllNodes returns a list of all monitored node IDs.
func (m *NodeMonitor) AllNodes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]string, 0, len(m.detectors))
	for id := range m.detectors {
		nodes = append(nodes, id)
	}
	return nodes
}

func usage() {
	log.Printf("Usage: nats-phi-detector [-s server] [-creds file] -mode <heartbeat|monitor> [options]\n")
	log.Printf("\nModes:\n")
	log.Printf("  heartbeat  Send heartbeats for a node\n")
	log.Printf("  monitor    Monitor nodes and calculate phi values\n")
	log.Printf("\nOptions:\n")
	flag.PrintDefaults()
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The NATS server URLs (separated by comma)")
	var userCreds = flag.String("creds", "", "User Credentials File")
	var mode = flag.String("mode", "", "Mode: 'heartbeat' or 'monitor'")
	var nodeID = flag.String("node", "", "Node ID to monitor or send heartbeats for")
	var subject = flag.String("subject", "heartbeat", "Base subject for heartbeats (node ID will be appended)")
	var interval = flag.Duration("interval", DefaultHeartbeatInterval, "Heartbeat interval (heartbeat mode)")
	var threshold = flag.Float64("threshold", DefaultPhiThreshold, "Phi threshold for failure detection (monitor mode)")
	var windowSize = flag.Int("window", DefaultWindowSize, "Sliding window size for statistics")
	var showHelp = flag.Bool("h", false, "Show help message")

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	if *mode == "" {
		log.Printf("Error: -mode is required\n")
		showUsageAndExit(1)
	}

	// Connect Options
	opts := []nats.Option{nats.Name("NATS Phi Accrual Detector")}
	opts = setupConnOptions(opts)

	if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
	}

	// Connect to NATS
	nc, err := nats.Connect(*urls, opts...)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	switch *mode {
	case "heartbeat":
		if *nodeID == "" {
			log.Printf("Error: -node is required for heartbeat mode\n")
			showUsageAndExit(1)
		}
		runHeartbeatSender(nc, *subject, *nodeID, *interval)

	case "monitor":
		runMonitor(nc, *subject, *nodeID, *threshold, *windowSize)

	default:
		log.Printf("Error: unknown mode '%s'\n", *mode)
		showUsageAndExit(1)
	}
}

// runHeartbeatSender sends periodic heartbeats for a node.
func runHeartbeatSender(nc *nats.Conn, baseSubject, nodeID string, interval time.Duration) {
	subject := fmt.Sprintf("%s.%s", baseSubject, nodeID)
	log.Printf("Sending heartbeats on [%s] every %v", subject, interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	seq := uint64(0)
	for {
		select {
		case <-ticker.C:
			seq++
			msg := fmt.Sprintf("%d:%d", seq, time.Now().UnixNano())
			if err := nc.Publish(subject, []byte(msg)); err != nil {
				log.Printf("Error publishing heartbeat: %v", err)
			} else {
				log.Printf("Sent heartbeat #%d", seq)
			}
			nc.Flush()

		case <-sigCh:
			log.Printf("Shutting down heartbeat sender")
			return
		}
	}
}

// runMonitor monitors heartbeats and calculates phi values.
func runMonitor(nc *nats.Conn, baseSubject, nodeID string, threshold float64, windowSize int) {
	monitor := NewNodeMonitor(nc, threshold, windowSize)

	// Subscribe to heartbeats
	var subject string
	if nodeID != "" {
		subject = fmt.Sprintf("%s.%s", baseSubject, nodeID)
	} else {
		subject = fmt.Sprintf("%s.*", baseSubject)
	}

	_, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		// Extract node ID from subject
		node := nodeID
		if node == "" {
			// Parse node ID from subject (last token)
			tokens := splitSubject(msg.Subject)
			if len(tokens) > 0 {
				node = tokens[len(tokens)-1]
			}
		}

		if node == "" {
			return
		}

		detector := monitor.GetOrCreateDetector(node)
		detector.Heartbeat()

		phi := detector.Phi()
		mean, stdDev := detector.Stats()
		status := "AVAILABLE"
		if phi >= threshold {
			status = "SUSPECTED FAILURE"
		}

		log.Printf("[%s] Heartbeat received | phi=%.3f | mean=%v | stddev=%v | status=%s",
			node, phi, mean.Round(time.Millisecond), stdDev.Round(time.Millisecond), status)
	})
	if err != nil {
		log.Fatalf("Error subscribing: %v", err)
	}
	nc.Flush()

	log.Printf("Monitoring heartbeats on [%s] with phi threshold %.1f", subject, threshold)
	log.Printf("Press Ctrl+C to exit")

	// Periodically check and report phi values
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for range ticker.C {
			for _, node := range monitor.AllNodes() {
				d := monitor.GetDetector(node)
				if d == nil {
					continue
				}

				phi := d.Phi()
				timeSince := d.TimeSinceLastHeartbeat()

				// Only report when phi is significant
				if phi >= 1.0 {
					status := "WARNING"
					if phi >= threshold {
						status = "FAILURE"
					}
					log.Printf("[%s] phi=%.3f | time_since_heartbeat=%v | status=%s",
						node, phi, timeSince.Round(time.Millisecond), status)
				}
			}
		}
	}()

	// Wait for signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("Shutting down monitor")
}

// splitSubject splits a NATS subject into tokens.
func splitSubject(subject string) []string {
	var tokens []string
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == '.' {
			if i > start {
				tokens = append(tokens, subject[start:i])
			}
			start = i + 1
		}
	}
	if start < len(subject) {
		tokens = append(tokens, subject[start:])
	}
	return tokens
}

func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		log.Printf("Disconnected due to: %s, will attempt reconnects for %.0fm", err, totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log.Fatalf("Exiting: %v", nc.LastError())
	}))
	return opts
}
