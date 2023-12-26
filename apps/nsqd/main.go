package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"syscall"

	"github.com/spf13/viper"
	"mlib.com/mlog"
	"mlib.com/mrun"
	"mlib.com/mskeleton"
	"mlib.com/nsq/internal/version"
	"mlib.com/nsq/nsqd"
)

type program struct {
	once sync.Once
	nsqd *nsqd.NSQD
}

func main() {
	prg := &program{}
	if err := mrun.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		mlog.Errorf("%v", err)
	}
}

func (p *program) Init(args ...interface{}) error {
	opts := nsqd.NewOptions()

	flagSet := nsqdFlagSet(opts)
	flagSet.Parse(os.Args[1:])

	// rand.Seed(time.Now().UTC().UnixNano())

	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) {
		fmt.Println(version.String("nsqd"))
		os.Exit(0)
	}

	var cfg config
	if flagSet.Lookup("config").Value.(flag.Getter).Get().(string) != "" {
		viper.SetConfigType("yaml")
		viper.SetConfigFile(flagSet.Lookup("config").Value.(flag.Getter).Get().(string))
		err := viper.ReadInConfig()
		if err != nil {
			mlog.Errorf("read nsqd configfile(%s) failed:%v", flagSet.Lookup("config").Value.(flag.Getter).Get().(string), err)
			return err
		}
	}
	cfg = viper.AllSettings()
	cfg.Validate()

	mskeleton.OptionResolve(opts, flagSet, cfg)

	nsqd, err := nsqd.New(opts)
	if err != nil {
		mlog.Errorf("failed to instantiate nsqd - %v", err)
		return err
	}
	p.nsqd = nsqd

	err = p.nsqd.LoadMetadata()
	if err != nil {
		mlog.Errorf("failed to load metadata - %v", err)
		return err
	}
	err = p.nsqd.PersistMetadata()
	if err != nil {
		mlog.Errorf("failed to persist metadata - %v", err)
		return err
	}

	go func() {
		err := p.nsqd.Main()
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
		p.nsqd.Exit()
		mskeleton.Destroy()
	})
}

func (p *program) Handle(s os.Signal) error {
	return fmt.Errorf("stop")
}

// // Context returns a context that will be canceled when nsqd initiates the shutdown
// func (p *program) Context() context.Context {
// 	return p.nsqd.Context()
// }

func (p *program) UserData() interface{} {
	return p
}
