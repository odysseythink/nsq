package main

import (
	"crypto/tls"
	"testing"

	"github.com/spf13/viper"
	"mlib.com/mskeleton"
	"mlib.com/nsq/internal/lg"
	"mlib.com/nsq/internal/test"
	"mlib.com/nsq/nsqd"
)

func TestConfigFlagParsing(t *testing.T) {
	opts := nsqd.NewOptions()
	opts.Logger = test.NewTestLogger(t)

	flagSet := nsqdFlagSet(opts)
	flagSet.Parse([]string{})

	var cfg config
	viper.SetConfigFile("../../contrib/nsqd.cfg.example.yaml")
	viper.SetConfigType("yaml")
	err := viper.ReadInConfig()
	if err != nil {
		t.Fatalf("%s", err)
	}
	cfg = viper.AllSettings()
	cfg["log_level"] = "debug"
	cfg.Validate()

	mskeleton.OptionResolve(opts, flagSet, cfg)
	nsqd.New(opts)

	if opts.TLSMinVersion != tls.VersionTLS10 {
		t.Errorf("min %#v not expected %#v", opts.TLSMinVersion, tls.VersionTLS10)
	}
	if opts.LogLevel != lg.DEBUG {
		t.Fatalf("log level: want debug, got %s", opts.LogLevel.String())
	}
}
