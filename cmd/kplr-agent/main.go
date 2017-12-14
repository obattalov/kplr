package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"os/signal"
	"syscall"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/geyser"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/model/k8s"
)

type (
	args struct {
		config string
		debug  bool
		ver    bool
		help   bool
	}

	config struct {
		Ingestor  *IngestorConfig `json:"ingestor"`
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

func mapMeta(ev *geyser.Event, cfg *IngestorConfig) model.Event {
	var egm k8s.EgMeta
	egm.SrcId = cfg.SourceId
	return egm.Event()
}

func mapRecords(ev *geyser.Event) []string {
	var lines []string
	for _, r := range ev.Records.Data {
		lines = append(lines, string(r))
	}
	return lines
}

func run(cfg *config, ctx context.Context, ing *Ingestor, gsr *geyser.Collector, done chan<- bool) error {
	go func() {
		defer func() {
			logger.Info("Exiting...")
			gsr.Stop()
			ing.Close()
			close(done)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-gsr.Events():
				for {
					err := ing.Ingest(mapMeta(ev, cfg.Ingestor), mapRecords(ev))
					if err == nil {
						break
					}
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Second):
						logger.Info("Ingest error, recovering; cause: ", err)
						ing.Close()
						ing, err = newIngestor(cfg.Ingestor, k8s.MetaDesc)
					}
				}
			}
		}
	}()
	return nil
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
	ing, err := newIngestor(cfg.Ingestor, k8s.MetaDesc)
	if err != nil {
		logger.Fatal("Unable to create ingestor; cause: ", err)
		return
	}
	gsr, err := newCollector(cfg.Collector)
	if err != nil {
		logger.Fatal("Unable to create collector; cause: ", err)
		return
	}

	done := make(chan bool)
	ctx, cancel := context.WithCancel(context.Background())
	if err := run(cfg, ctx, ing, gsr, done); err != nil {
		return
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-sigChan:
		logger.Warn("Handling signal=", s)
		cancel()
		<-done
	}
}
