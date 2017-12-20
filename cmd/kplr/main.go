package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/jrivets/inject"
	"github.com/jrivets/log4g"
	"github.com/kplr-io/kplr"
	"github.com/kplr-io/kplr/api"
	"github.com/kplr-io/kplr/cursor"
	"github.com/kplr-io/kplr/index"
	"github.com/kplr-io/kplr/journal"
	"github.com/kplr-io/kplr/kingpin_addons"
	"github.com/kplr-io/kplr/mpool"
	"github.com/kplr-io/kplr/zebra"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	cfg, err := parseCLP()
	if err != nil {
		return
	}
	defer log4g.Shutdown()

	if cfg == nil {
		return
	}

	kplr.DefaultLogger.Info("Kepler is starting...")
	injector := inject.NewInjector(log4g.GetLogger("kplr.injector"), log4g.GetLogger("fb.injector"))

	mainCtx, cancel := context.WithCancel(context.Background())
	defer kplr.DefaultLogger.Info("Exiting. kplr main context is shutdown.")
	defer injector.Shutdown()
	defer cancel()

	mpl := mpool.NewPool()
	transp := zebra.NewTransport(&zebra.TransportConfig{
		ListenAddress:  cfg.ListenOn,
		SessTimeoutSec: kplr.GetIntVal(cfg.SessionTimeoutSec, 0),
	})
	jctrlr := journal.NewJournalCtrlr()

	rapi := api.NewRestApi()
	cprvdr := cursor.NewCursorProvider()
	ttbl := index.NewTTable()

	injector.RegisterOne(jctrlr, "")
	injector.RegisterOne(transp, "")
	injector.RegisterOne(mpl, "mPool")
	injector.RegisterOne(mainCtx, "mainCtx")
	injector.RegisterOne(cfg, "restApiConfig")
	injector.RegisterOne(cfg, "tableConfig")
	injector.RegisterOne(cfg, "journalConfig")
	injector.RegisterOne(rapi, "")
	injector.RegisterOne(cprvdr, "")
	injector.RegisterOne(ttbl, "tTable")
	injector.Construct()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	select {
	case <-signalChan:
		kplr.DefaultLogger.Warn("Interrupt signal is received")
	}
}

// parseCLP parses Command Line Params and returns config
func parseCLP() (*Config, error) {
	var (
		debug_api   = kingpin.Flag("debug-api", "Enable debug mode for ReST API calls.").Bool()
		maxJrnlSize = kingpin_addons.Size(kingpin.Flag("journal-max-size", "Specifies maximum journal size (100G, 50M etc.)").Default("1Tb"))
		maxChnkSize = kingpin_addons.Size(kingpin.Flag("chunk-max-size", "Specifies maximum chunk size (100G, 50M etc.)").Default("50Mb"))
		cfgFile     = kingpin.Flag("config-file", "The kplr configuration file name").Default(defaultConfigFile).String()
		logCfgFile  = kingpin.Flag("log-config", "The log4g configuration file name").Default(defaultLog4gCongigFile).String()
		jrnlsDir    = kingpin.Flag("journals-dir", "The directory where journals will be stored").Default(defaultKplrJrnlsDir).String()
		pCfg        = &Config{}
	)
	kingpin.Version("0.0.1")
	kingpin.Parse()

	if *logCfgFile != "" {
		if kplr.IsFileNotExist(*logCfgFile) {
			kplr.DefaultLogger.Warn("No file ", logCfgFile, " will use default log4g configuration")
		} else {
			err := log4g.ConfigF(*logCfgFile)
			if err != nil {
				kingpin.FatalIfError(err, "Could not parse %s file as a log4g configuration, please check syntax ", *logCfgFile)
			}
		}
	}

	pCfg.HttpDebugMode = *debug_api
	pCfg.JrnlMaxSize = *maxJrnlSize
	pCfg.JrnlChunkMaxSize = *maxChnkSize
	pCfg.JournalsDir = *jrnlsDir

	if pCfg.JrnlMaxSize <= pCfg.JrnlChunkMaxSize {
		kingpin.Fatalf("Misconfiguration. Journal max size %s must be greater than journal's chunk size, which is %s",
			kplr.FormatSize(pCfg.JrnlMaxSize), kplr.FormatSize(pCfg.JrnlChunkMaxSize))
	}

	if pCfg.JrnlMaxSize <= 2*pCfg.JrnlChunkMaxSize {
		kingpin.Fatalf("Possible misconfiguration. The journal max size is %s which is pretty close to journal's chunk size %s. Please check documentation to be sure it is ok for you.",
			kplr.FormatSize(pCfg.JrnlMaxSize), kplr.FormatSize(pCfg.JrnlChunkMaxSize))
	}

	// file config
	fCfg := &Config{}
	fCfg.readFromFile(*cfgFile)

	// Final config - default, then from file and then params
	cfg := newDefaultConfig()
	cfg.Apply(fCfg)
	cfg.Apply(pCfg)
	return cfg, nil
}
