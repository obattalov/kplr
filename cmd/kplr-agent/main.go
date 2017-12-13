package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

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

	backend struct {
		Address          string `json:"address"`
		PartitionId      string `json:"partitionId"`
		PacketMaxRecords int    `json:"packetMaxRecords"`
	}

	config struct {
		Backend   *backend       `json:"backend"`
		Collector *geyser.Config `json:"collector"`
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

	gsr, err := geyser.NewCollector(cfg.Collector,
		logger.WithId(".collector").(log4g.Logger))
	if err != nil {
		logger.Fatal("Unable to create collector; cause: ", err)
		return
	}
	defer func() {
		logger.Info("Exiting...")
		gsr.Stop()
	}()

	if err := gsr.Start(); err != nil {
		logger.Fatal("Failed to start agent; cause: ", err)
		return
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-sigChan:
		logger.Warn("Handling signal=", s)
	}
}
