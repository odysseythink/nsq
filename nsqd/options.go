package nsqd

import (
	"crypto/md5"
	"crypto/tls"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"

	"mlib.com/nsq/internal/lg"
)

type Options struct {
	// basic options
	ID        int64       `flag:"node-id" cfg:"id" yaml:"id"`
	LogLevel  lg.LogLevel `flag:"log-level" yaml:"log-level"`
	LogPrefix string      `flag:"log-prefix" yaml:"log-prefix"`
	Logger    Logger

	TCPAddress               string        `flag:"tcp-address" yaml:"tcp-address"`
	HTTPAddress              string        `flag:"http-address" yaml:"http-address"`
	HTTPSAddress             string        `flag:"https-address" yaml:"https-address"`
	RaftAddress              string        `flag:"raft-address" yaml:"raft-address"`
	BroadcastAddress         string        `flag:"broadcast-address" yaml:"broadcast-address"`
	BroadcastTCPPort         int           `flag:"broadcast-tcp-port" yaml:"broadcast-tcp-port"`
	BroadcastHTTPPort        int           `flag:"broadcast-http-port" yaml:"broadcast-http-port"`
	NSQLookupdTCPAddresses   []string      `flag:"lookupd-tcp-address" cfg:"nsqlookupd_tcp_addresses" yaml:"lookupd-tcp-address"`
	AuthHTTPAddresses        []string      `flag:"auth-http-address" cfg:"auth_http_addresses" yaml:"auth_http_addresses"`
	HTTPClientConnectTimeout time.Duration `flag:"http-client-connect-timeout" cfg:"http_client_connect_timeout" yaml:"http_client_connect_timeout"`
	HTTPClientRequestTimeout time.Duration `flag:"http-client-request-timeout" cfg:"http_client_request_timeout" yaml:"http_client_request_timeout"`

	// diskqueue options
	DataPath        string        `flag:"data-path" yaml:"data-path"`
	MemQueueSize    int64         `flag:"mem-queue-size" yaml:"mem-queue-size"`
	MaxBytesPerFile int64         `flag:"max-bytes-per-file" yaml:"max-bytes-per-file"`
	SyncEvery       int64         `flag:"sync-every" yaml:"sync-every"`
	SyncTimeout     time.Duration `flag:"sync-timeout" yaml:"sync-timeout"`

	QueueScanInterval        time.Duration
	QueueScanRefreshInterval time.Duration
	QueueScanSelectionCount  int `flag:"queue-scan-selection-count" yaml:"queue-scan-selection-count"`
	QueueScanWorkerPoolMax   int `flag:"queue-scan-worker-pool-max" yaml:"queue-scan-worker-pool-max"`
	QueueScanDirtyPercent    float64

	// msg and command options
	MsgTimeout    time.Duration `flag:"msg-timeout" yaml:"msg-timeout"`
	MaxMsgTimeout time.Duration `flag:"max-msg-timeout" yaml:"max-msg-timeout"`
	MaxMsgSize    int64         `flag:"max-msg-size" yaml:"max-msg-size"`
	MaxBodySize   int64         `flag:"max-body-size" yaml:"max-body-size"`
	MaxReqTimeout time.Duration `flag:"max-req-timeout" yaml:"max-req-timeout"`
	ClientTimeout time.Duration

	// client overridable configuration options
	MaxHeartbeatInterval   time.Duration `flag:"max-heartbeat-interval" yaml:"max-heartbeat-interval"`
	MaxRdyCount            int64         `flag:"max-rdy-count" yaml:"max-rdy-count"`
	MaxOutputBufferSize    int64         `flag:"max-output-buffer-size" yaml:"max-output-buffer-size"`
	MaxOutputBufferTimeout time.Duration `flag:"max-output-buffer-timeout" yaml:"max-output-buffer-timeout"`
	MinOutputBufferTimeout time.Duration `flag:"min-output-buffer-timeout" yaml:"min-output-buffer-timeout"`
	OutputBufferTimeout    time.Duration `flag:"output-buffer-timeout" yaml:"output-buffer-timeout"`
	MaxChannelConsumers    int           `flag:"max-channel-consumers" yaml:"max-channel-consumers"`

	// statsd integration
	StatsdAddress          string        `flag:"statsd-address" yaml:"statsd-address"`
	StatsdPrefix           string        `flag:"statsd-prefix" yaml:"statsd-prefix"`
	StatsdInterval         time.Duration `flag:"statsd-interval" yaml:"statsd-interval"`
	StatsdMemStats         bool          `flag:"statsd-mem-stats" yaml:"statsd-mem-stats"`
	StatsdUDPPacketSize    int           `flag:"statsd-udp-packet-size" yaml:"statsd-udp-packet-size"`
	StatsdExcludeEphemeral bool          `flag:"statsd-exclude-ephemeral" yaml:"statsd-exclude-ephemeral"`

	// e2e message latency
	E2EProcessingLatencyWindowTime  time.Duration `flag:"e2e-processing-latency-window-time" yaml:"e2e-processing-latency-window-time"`
	E2EProcessingLatencyPercentiles []float64     `flag:"e2e-processing-latency-percentile" cfg:"e2e_processing_latency_percentiles" yaml:"e2e-processing-latency-percentile"`

	// TLS config
	TLSCert             string `flag:"tls-cert" yaml:"tls-cert"`
	TLSKey              string `flag:"tls-key" yaml:"tls-key"`
	TLSClientAuthPolicy string `flag:"tls-client-auth-policy" yaml:"tls-client-auth-policy"`
	TLSRootCAFile       string `flag:"tls-root-ca-file" yaml:"tls-root-ca-file"`
	TLSRequired         int    `flag:"tls-required" yaml:"tls-required"`
	TLSMinVersion       uint16 `flag:"tls-min-version" yaml:"tls-min-version"`

	// compression
	DeflateEnabled  bool `flag:"deflate" yaml:"deflate"`
	MaxDeflateLevel int  `flag:"max-deflate-level" yaml:"max-deflate-level"`
	SnappyEnabled   bool `flag:"snappy" yaml:"snappy"`
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	io.WriteString(h, hostname)
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Options{
		ID:        defaultID,
		LogPrefix: "[nsqd] ",
		LogLevel:  lg.INFO,

		TCPAddress:        "0.0.0.0:4150",
		HTTPAddress:       "0.0.0.0:4151",
		HTTPSAddress:      "0.0.0.0:4152",
		BroadcastAddress:  hostname,
		BroadcastTCPPort:  0,
		BroadcastHTTPPort: 0,

		NSQLookupdTCPAddresses: make([]string, 0),
		AuthHTTPAddresses:      make([]string, 0),

		HTTPClientConnectTimeout: 2 * time.Second,
		HTTPClientRequestTimeout: 5 * time.Second,

		MemQueueSize:    10000,
		MaxBytesPerFile: 100 * 1024 * 1024,
		SyncEvery:       2500,
		SyncTimeout:     2 * time.Second,

		QueueScanInterval:        100 * time.Millisecond,
		QueueScanRefreshInterval: 5 * time.Second,
		QueueScanSelectionCount:  20,
		QueueScanWorkerPoolMax:   4,
		QueueScanDirtyPercent:    0.25,

		MsgTimeout:    60 * time.Second,
		MaxMsgTimeout: 15 * time.Minute,
		MaxMsgSize:    1024 * 1024,
		MaxBodySize:   5 * 1024 * 1024,
		MaxReqTimeout: 1 * time.Hour,
		ClientTimeout: 60 * time.Second,

		MaxHeartbeatInterval:   60 * time.Second,
		MaxRdyCount:            2500,
		MaxOutputBufferSize:    64 * 1024,
		MaxOutputBufferTimeout: 30 * time.Second,
		MinOutputBufferTimeout: 25 * time.Millisecond,
		OutputBufferTimeout:    250 * time.Millisecond,
		MaxChannelConsumers:    0,

		StatsdPrefix:        "nsq.%s",
		StatsdInterval:      60 * time.Second,
		StatsdMemStats:      true,
		StatsdUDPPacketSize: 508,

		E2EProcessingLatencyWindowTime: time.Duration(10 * time.Minute),

		DeflateEnabled:  true,
		MaxDeflateLevel: 6,
		SnappyEnabled:   true,

		TLSMinVersion: tls.VersionTLS10,
	}
}
