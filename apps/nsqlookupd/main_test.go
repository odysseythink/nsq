package main

import (
	"testing"

	"mlib.com/mskeleton"
	"mlib.com/nsq/internal/lg"
	"mlib.com/nsq/internal/test"
	"mlib.com/nsq/nsqlookupd"
)

func TestConfigFlagParsing(t *testing.T) {
	opts := nsqlookupd.NewOptions()
	opts.Logger = test.NewTestLogger(t)

	flagSet := nsqlookupdFlagSet(opts)
	flagSet.Parse([]string{})

	cfg := config{"log_level": "debug"}
	cfg.Validate()

	mskeleton.OptionResolve(opts, flagSet, cfg)
	if opts.LogLevel != lg.DEBUG {
		t.Fatalf("log level: want debug, got %s", opts.LogLevel.String())
	}
}
