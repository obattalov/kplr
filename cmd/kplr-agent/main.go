package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/geyser"
	"github.com/kplr-io/kplr"
	"gopkg.in/alecthomas/kingpin.v2"
)

type (
	args struct {
		config      string
		debug       bool
		printStatus bool
	}

	config struct {
		Ingestor   *ingestorConfig `json:"ingestor"`
		Collector  *geyser.Config  `json:"collector"`
		StatusFile string          `json:"statusFile"`
	}
)

const (
	Version              = "0.0.1"
	DefaultConfigPath    = "/opt/kplr/config.json"
	cDefaultConfigStatus = "/tmp/kplr-agent.status"
	cStatFileUpdateSec   = 5 * time.Second
)

var (
	logger = log4g.GetLogger("kplr.agent")
)

func loadConfig(path string) (*config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &config{}
	err = json.Unmarshal(data, cfg)
	if err == nil && cfg.StatusFile == "" {
		cfg.StatusFile = cDefaultConfigStatus
	}
	return cfg, err
}

func newCollector(cfg *geyser.Config) (*geyser.Collector, error) {
	gsr, err := geyser.NewCollector(cfg)
	if err == nil {
		err = gsr.Start()
	}
	return gsr, err
}

func runIngest(ctx context.Context, ing *ingestor, events <-chan *geyser.Event, done chan<- bool) {
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			select {
			case <-ctx.Done():
				return
			case ev := <-events:
				var err error
				for ctx.Err() == nil {
					if err == nil {
						err = ing.ingest(ev)
						if err == nil {
							ev.Confirm()
							break
						}
					}
					logger.Info("Ingestor error, recovering; cause: ", err)
					err = ing.connect()
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
		log4g.SetLogLevel("", log4g.TRACE)
	}

	cfg, err := loadConfig(args.config)
	if err != nil {
		cfg = getDefaultConfig()
		logger.Warn("Unable to load config file=", args.config, "; cause: ", err, " will use default one")
	}

	if args.printStatus {
		printStatFile(cfg)
		os.Exit(0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		select {
		case s := <-sigChan:
			logger.Warn("Handling signal=", s)
			cancel()
		}
	}()

	var (
		gsr *geyser.Collector
		ing *ingestor
	)

	gsr, err = newCollector(cfg.Collector)
	if err != nil {
		logger.Fatal("Unable to create collector; cause: ", err)
		return
	}
	defer gsr.Stop()

	ing, err = newIngestor(cfg.Ingestor, ctx)
	if err != nil {
		logger.Fatal("Unable to create ingestor; cause: ", err)
		return
	}
	defer ing.close()

	if cfg.StatusFile != "" {
		go func() {
			logger.Info("Will update status every ", cStatFileUpdateSec, " to ", cfg.StatusFile)
			defer deleteStatFile(cfg)
			for {
				saveStatFile(cfg, gsr, ing)
				select {
				case <-ctx.Done():
					logger.Info("Stop writing stat file")
					return
				case <-time.After(cStatFileUpdateSec):
				}
			}
		}()
	} else {
		logger.Warn("Will not update agent status, the status file is empty in config!")
	}

	ing.connect()

	done := make(chan bool)
	runIngest(ctx, ing, gsr.Events(), done)
	<-done
}

func getDefaultConfig() *config {
	sc := []*schemaConfig{
		&schemaConfig{
			PathMatcher: "/*(?:.+/)*(?P<file>.+\\..+)",
			SourceId:    "{file}",
			Tags:        map[string]string{"file": "{file}"},
		},
	}
	cfg := &config{}
	cfg.Ingestor = &ingestorConfig{}
	cfg.Ingestor.Server = "127.0.0.1:9966"
	cfg.Ingestor.PacketMaxRecords = 1000
	cfg.Ingestor.HeartBeatMs = 15000
	cfg.Ingestor.AccessKey = ""
	cfg.Ingestor.SecretKey = ""
	cfg.Ingestor.Schemas = sc
	cfg.Collector = geyser.NewDefaultConfig()
	cfg.StatusFile = cDefaultConfigStatus
	return cfg
}

func parseArgs() *args {
	var (
		config = kingpin.Flag("config-file", "The kplr-agent configuration file name").Default(DefaultConfigPath).String()
		debug  = kingpin.Flag("debug", "Enable debug log level").Bool()
		status = kingpin.Flag("print-status", "Prints status of the agent, if it is already run").Bool()
	)
	kingpin.Version(Version)
	kingpin.Parse()

	res := new(args)
	res.config = *config
	res.debug = *debug
	res.printStatus = *status
	return res
}

func saveStatFile(cfg *config, gsr *geyser.Collector, ing *ingestor) {
	var w bytes.Buffer
	tw := new(tabwriter.Writer)
	tw.Init(&w, 0, 8, 1, ' ', 0)

	fmt.Fprintf(tw, "*************\n")
	fmt.Fprintf(tw, "* Kplr Agent \n")
	fmt.Fprintf(tw, "*************\n")
	fmt.Fprintf(tw, "Status at %s\n\n", time.Now().String())
	fmt.Fprintf(tw, "=== Connection ===\n")
	fmt.Fprintf(tw, "\tAggregator:\t%s\n", cfg.Ingestor.Server)
	fmt.Fprintf(tw, "\tRecs per packet:\t%d\n", cfg.Ingestor.PacketMaxRecords)
	fmt.Fprintf(tw, "\tHeartbeat:\t%dms\n", cfg.Ingestor.HeartBeatMs)

	if ing.zClient == nil {
		fmt.Fprintf(tw, "\tStatus:\tCONNECTING...\n")
	} else {
		fmt.Fprintf(tw, "\tStatus:\tCONNECTED\n")
	}

	fmt.Fprintf(tw, "\n=== Collector ===\n")

	gs := gsr.GetStats()
	fmt.Fprintf(tw, "\tscan paths:\t%v\n", gs.Config.ScanPaths)
	fmt.Fprintf(tw, "\tscan intervals:\tevery %d sec.\n", gs.Config.ScanPathsIntervalSec)
	fmt.Fprintf(tw, "\tstate file:\t%s\n", gs.Config.StateFile)
	fmt.Fprintf(tw, "\tstate update:\tevery %d sec.\n", gs.Config.StateFlushIntervalSec)
	fmt.Fprintf(tw, "\tfile formats:\t%v\n", gs.Config.FileFormats)
	fmt.Fprintf(tw, "\trecord max size:\t%s\n", kplr.FormatSize(int64(gs.Config.RecordMaxSizeBytes)))
	fmt.Fprintf(tw, "\trecords per pack:\t%d\n\n", gs.Config.EventMaxRecords)
	fmt.Fprintf(tw, "--- scanned files (%d) ---\n", len(gs.Workers))
	for _, wkr := range gs.Workers {
		fmt.Fprintf(tw, "\t%s\n", wkr.Filename)
	}

	knwnTags := ing.getKnownTags()
	totalPerc := float64(0)
	for i, wkr := range gs.Workers {
		fmt.Fprintf(tw, "\n--- Scanner %d\n", i+1)
		fmt.Fprintf(tw, "\t%s\n", wkr.Filename)
		fmt.Fprintf(tw, "\tdata-type:\t%s\n", wkr.ParserStats.DataType)
		size := wkr.ParserStats.Size
		fmt.Fprintf(tw, "\tsize:\t%s\n", kplr.FormatSize(size))

		pos := wkr.ParserStats.Pos
		perc := float64(100)
		if size > 0 {
			perc = float64(pos) * perc / float64(size)
		}
		totalPerc += perc

		if tags, ok := knwnTags[wkr.Filename]; ok {
			hdrs := tags.(*hdrsCacheRec)
			fmt.Fprintf(tw, "\tknwnTags: \n\tsrcId=%s, tags=%s\n", hdrs.srcId, hdrs.tags)
		} else {
			fmt.Fprintf(tw, "\tknwnTags:\t<data is not sent yet, or no new data for 5 mins>\n")
		}

		fmt.Fprintf(tw, "\tprogress:\t%s %s\n", kplr.FormatSize(pos), kplr.FormatProgress(30, perc))
		if len(wkr.ParserStats.DateFormats) > 0 {
			fmt.Fprintf(tw, "\n\tFormats:\n")
			tot := int64(0)
			for _, v := range wkr.ParserStats.DateFormats {
				tot += v
			}
			for dtf, v := range wkr.ParserStats.DateFormats {
				perc := float64(v) * 100.0 / float64(tot)
				fmt.Fprintf(tw, "\t\t\"%s\"\t%5.2f%%(%d of %d records have the format)\n", dtf, perc, v, tot)
			}
		}
		fmt.Fprintf(tw, "-----------\n")
	}
	if len(gs.Workers) > 0 {
		totalPerc /= float64(len(gs.Workers))
	}
	fmt.Fprintf(tw, "\nReplica status: %s\n", kplr.FormatProgress(40, totalPerc))

	tw.Flush()
	ioutil.WriteFile(cfg.StatusFile, []byte(w.Bytes()), 0644)
}

func deleteStatFile(cfg *config) {
	os.Remove(cfg.StatusFile)
}

func printStatFile(cfg *config) {
	res, err := ioutil.ReadFile(cfg.StatusFile)
	if err != nil {
		fmt.Println("ERROR: Seems no agent running.")
		return
	}
	fmt.Println(string(res))
}
