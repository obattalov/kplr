package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/geyser"
)

type (
	args struct {
		config string
		debug  bool
		ver    bool
		help   bool
	}

	config struct {
		Ingestor  *ingestorConfig `json:"ingestor"`
		Collector *geyser.Config  `json:"collector"`
	}
)

const (
	Version           = "0.0.1"
	DefaultConfigPath = "/opt/kplr/agent.json"
)

var (
	logger = log4g.GetLogger("kplr.agent")
)

func parseArgs() *args {
	args := &args{}

	flag.StringVar(&args.config, "config", DefaultConfigPath, "specify config file location")
	flag.BoolVar(&args.debug, "debug", false, "enable debug")
	flag.BoolVar(&args.ver, "version", false, "show version")
	flag.BoolVar(&args.help, "help", false, "show help")
	flag.Parse()

	return args
}

func version() {
	fmt.Println("Version:", Version)
}

func loadConfig(path string) (*config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &config{}
	err = json.Unmarshal(data, cfg)
	return cfg, err
}

func newCollector(cfg *geyser.Config) (*geyser.Collector, error) {
	gsr, err := geyser.NewCollector(cfg)
	if err == nil {
		err = gsr.Start()
	}
	return gsr, nil
}

func runIngest(ctx context.Context, ing *ingestor, events <-chan *geyser.Event, done chan<- bool) {
	go func() {
		defer func() {
			close(done)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-events:
				var err error
				for {
					if err == nil {
						//TODO: confirm msg delivery
						err = ing.ingest(ev)
						if err == nil {
							break
						}
					}
					logger.Info("Ingestor error, recovering; cause: ", err)
					err = ing.reInit()
					if !wait(ctx, 1) {
						break
					}
				}
			}
		}
	}()
}

func wait(ctx context.Context, timeoutSec int) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(time.Duration(timeoutSec) * time.Second):
		return true
	}
}

func main() {
	defer log4g.Shutdown()
	args := parseArgs()
	if args.debug {
		log4g.SetLogLevel("", log4g.DEBUG)
	}
	if args.help {
		flag.Usage()
		os.Exit(0)
	}
	if args.ver {
		version()
		os.Exit(0)
	}

	cfg, err := loadConfig(args.config)
	if err != nil {
		logger.Fatal("Unable to load config file=", args.config, "; cause: ", err)
		return
	}

	ing, err := newIngestor(cfg.Ingestor)
	if err != nil {
		logger.Fatal("Unable to create ingestor; cause: ", err)
		return
	}

	defer ing.close()
	gsr, err := newCollector(cfg.Collector)
	if err != nil {
		logger.Fatal("Unable to create collector; cause: ", err)
		return
	}

	defer gsr.Stop()
	done := make(chan bool)
	ctx, cancel := context.WithCancel(context.Background())
	runIngest(ctx, ing, gsr.Events(), done)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-sigChan:
		logger.Warn("Handling signal=", s)
		cancel()
		<-done
	}
}
