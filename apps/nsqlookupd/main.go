package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"syscall"

	"mlib.com/mrun"
	"mlib.com/mskeleton"
	"mlib.com/nsq/internal/lg"
	"mlib.com/nsq/internal/version"
	"mlib.com/nsq/nsqlookupd"
)

func nsqlookupdFlagSet(opts *nsqlookupd.Options) *flag.FlagSet {
	flagSet := flag.NewFlagSet("nsqlookupd", flag.ExitOnError)

	flagSet.String("config", "", "path to config file")
	flagSet.Bool("version", false, "print version string")

	logLevel := opts.LogLevel
	flagSet.Var(&logLevel, "log-level", "set log verbosity: debug, info, warn, error, or fatal")
	flagSet.String("log-prefix", "[nsqlookupd] ", "log message prefix")
	flagSet.Bool("verbose", false, "[deprecated] has no effect, use --log-level")

	flagSet.String("tcp-address", opts.TCPAddress, "<addr>:<port> to listen on for TCP clients")
	flagSet.String("http-address", opts.HTTPAddress, "<addr>:<port> to listen on for HTTP clients")
	flagSet.String("broadcast-address", opts.BroadcastAddress, "address of this lookupd node, (default to the OS hostname)")

	flagSet.Duration("inactive-producer-timeout", opts.InactiveProducerTimeout, "duration of time a producer will remain in the active list since its last ping")
	flagSet.Duration("tombstone-lifetime", opts.TombstoneLifetime, "duration of time a producer will remain tombstoned if registration remains")

	return flagSet
}

type program struct {
	once       sync.Once
	nsqlookupd *nsqlookupd.NSQLookupd
}

func main() {
	prg := &program{}
	if err := mrun.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%s", err)
	}
}

func (p *program) Init(args ...interface{}) error {
	opts := nsqlookupd.NewOptions()

	flagSet := nsqlookupdFlagSet(opts)
	flagSet.Parse(os.Args[1:])

	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqlookupd"))
		os.Exit(0)
	}

	var cfg config
	// configFile := flagSet.Lookup("config").Value.String()
	cfg.Validate()

	mskeleton.OptionResolve(opts, flagSet, cfg)
	nsqlookupd, err := nsqlookupd.New(opts)
	if err != nil {
		logFatal("failed to instantiate nsqlookupd", err)
	}
	p.nsqlookupd = nsqlookupd

	go func() {
		err := p.nsqlookupd.Main()
		if err != nil {
			p.Destroy()
			os.Exit(1)
		}
	}()

	return nil
}

func (p *program) RunOnce(ctx context.Context) error {
	return nil
}

func (p *program) Destroy() {
	p.once.Do(func() {
		p.nsqlookupd.Exit()
	})
}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqlookupd] ", f, args...)
}

func (p *program) UserData() interface{} {
	return p
}
