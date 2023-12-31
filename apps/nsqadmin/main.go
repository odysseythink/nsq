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
	"mlib.com/nsq/internal/app"
	"mlib.com/nsq/internal/lg"
	"mlib.com/nsq/internal/version"
	"mlib.com/nsq/nsqadmin"
)

func nsqadminFlagSet(opts *nsqadmin.Options) *flag.FlagSet {
	flagSet := flag.NewFlagSet("nsqadmin", flag.ExitOnError)

	flagSet.String("config", "", "path to config file")
	flagSet.Bool("version", false, "print version string")

	logLevel := opts.LogLevel
	flagSet.Var(&logLevel, "log-level", "set log verbosity: debug, info, warn, error, or fatal")
	flagSet.String("log-prefix", "[nsqadmin] ", "log message prefix")
	flagSet.Bool("verbose", false, "[deprecated] has no effect, use --log-level")

	flagSet.String("http-address", opts.HTTPAddress, "<addr>:<port> to listen on for HTTP clients")
	flagSet.String("base-path", opts.BasePath, "URL base path")

	flagSet.String("graphite-url", opts.GraphiteURL, "graphite HTTP address")
	flagSet.Bool("proxy-graphite", false, "proxy HTTP requests to graphite")

	flagSet.String("statsd-counter-format", opts.StatsdCounterFormat, "The counter stats key formatting applied by the implementation of statsd. If no formatting is desired, set this to an empty string.")
	flagSet.String("statsd-gauge-format", opts.StatsdGaugeFormat, "The gauge stats key formatting applied by the implementation of statsd. If no formatting is desired, set this to an empty string.")
	flagSet.String("statsd-prefix", opts.StatsdPrefix, "prefix used for keys sent to statsd (%s for host replacement, must match nsqd)")
	flagSet.Duration("statsd-interval", opts.StatsdInterval, "time interval nsqd is configured to push to statsd (must match nsqd)")

	flagSet.String("notification-http-endpoint", "", "HTTP endpoint (fully qualified) to which POST notifications of admin actions will be sent")

	flagSet.Duration("http-client-connect-timeout", opts.HTTPClientConnectTimeout, "timeout for HTTP connect")
	flagSet.Duration("http-client-request-timeout", opts.HTTPClientRequestTimeout, "timeout for HTTP request")

	flagSet.Bool("http-client-tls-insecure-skip-verify", false, "configure the HTTP client to skip verification of TLS certificates")
	flagSet.String("http-client-tls-root-ca-file", "", "path to CA file for the HTTP client")
	flagSet.String("http-client-tls-cert", "", "path to certificate file for the HTTP client")
	flagSet.String("http-client-tls-key", "", "path to key file for the HTTP client")

	flagSet.String("allow-config-from-cidr", opts.AllowConfigFromCIDR, "A CIDR from which to allow HTTP requests to the /config endpoint")
	flagSet.String("acl-http-header", opts.AclHttpHeader, "HTTP header to check for authenticated admin users")

	nsqlookupdHTTPAddresses := app.StringArray{}
	flagSet.Var(&nsqlookupdHTTPAddresses, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	nsqdHTTPAddresses := app.StringArray{}
	flagSet.Var(&nsqdHTTPAddresses, "nsqd-http-address", "nsqd HTTP address (may be given multiple times)")
	adminUsers := app.StringArray{}
	flagSet.Var(&adminUsers, "admin-user", "admin user (may be given multiple times; if specified, only these users will be able to perform privileged actions; acl-http-header is used to determine the authenticated user)")

	return flagSet
}

type program struct {
	once     sync.Once
	nsqadmin *nsqadmin.NSQAdmin
}

func main() {
	prg := &program{}
	if err := mrun.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%s", err)
	}
}

func (p *program) Init(args ...interface{}) error {
	opts := nsqadmin.NewOptions()

	flagSet := nsqadminFlagSet(opts)
	flagSet.Parse(os.Args[1:])

	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqadmin"))
		os.Exit(0)
	}

	var cfg config
	cfg.Validate()

	mskeleton.OptionResolve(opts, flagSet, cfg)
	nsqadmin, err := nsqadmin.New(opts)
	if err != nil {
		logFatal("failed to instantiate nsqadmin - %s", err)
	}
	p.nsqadmin = nsqadmin

	go func() {
		err := p.nsqadmin.Main()
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
		p.nsqadmin.Exit()
	})
}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqadmin] ", f, args...)
}
func (p *program) UserData() interface{} {
	return p
}
