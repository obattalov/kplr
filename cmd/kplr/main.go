package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/jrivets/inject"
	"github.com/jrivets/log4g"
	"github.com/kplr-io/kplr"
	"github.com/kplr-io/kplr/api"
	"github.com/kplr-io/kplr/journal"
	"github.com/kplr-io/kplr/model/k8s"
	"github.com/kplr-io/kplr/mpool"
	"github.com/kplr-io/kplr/wire"
)

func main() {
	cfg, err := prepareConfig()
	defer log4g.Shutdown()

	if err != nil {
		kplr.DefaultLogger.Error(err)
		return
	}

	if cfg == nil {
		return
	}

	kplr.DefaultLogger.Info("Kepler is starting...")
	injector := inject.NewInjector(log4g.GetLogger("pixty.injector"), log4g.GetLogger("fb.injector"))

	mainCtx, cancel := context.WithCancel(context.Background())
	defer kplr.DefaultLogger.Info("Exiting. kplr main context is shutdown.")
	defer injector.Shutdown()
	defer cancel()

	mpl := mpool.NewPool()
	mDesc := k8s.NewDescriptor()
	transp := wire.NewTransport(&wire.TransportConfig{
		ListenAddress:  cfg.ListenOn,
		SessTimeoutSec: kplr.GetIntVal(cfg.SessionTimeoutSec, 0),
	})
	jctrlr := journal.NewJournalCtrlr(&journal.JournalConfig{Dir: cfg.JournalsDir})

	rapi := api.NewRestApi()

	injector.RegisterOne(jctrlr, "")
	injector.RegisterOne(transp, "")
	injector.RegisterOne(mDesc, "mdlDesc")
	injector.RegisterOne(mpl, "mPool")
	injector.RegisterOne(mainCtx, "mainCtx")
	injector.RegisterOne(cfg, "restApiConfig")
	injector.RegisterOne(rapi, "")
	injector.Construct()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	select {
	case <-signalChan:
		kplr.DefaultLogger.Warn("Interrupt signal is received")
	}
}

func prepareConfig() (*Config, error) {
	var (
		help       bool
		cfgFile    string
		logCfgFile string
		pCfg       = &Config{}
	)

	flag.StringVar(&cfgFile, "config-file", defaultConfigFile, "The kplr configuration file name")
	flag.StringVar(&logCfgFile, "log-config", defaultLog4gCongigFile, "The log4g configuration file name")
	flag.StringVar(&pCfg.JournalsDir, "journal-dir", defaultKplrJrnlsDir, "The directory where journal will be stored")
	flag.BoolVar(&help, "help", false, "Prints the usage")

	flag.Parse()

	if help {
		fmt.Fprintf(os.Stderr, "%s is kplr daemon which serves Log Aggregator requests\n", os.Args[0])
		flag.Usage()
		return nil, nil
	}

	if logCfgFile != "" {
		if kplr.IsFileNotExist(logCfgFile) {
			kplr.DefaultLogger.Warn("No file ", logCfgFile, " will use default log4g configuration")
		} else {
			err := log4g.ConfigF(logCfgFile)
			if err != nil {
				panic(err)
			}
		}
	}

	// file config
	fCfg := &Config{}
	fCfg.readFromFile(cfgFile)

	// Final config - default, then from file and then params
	cfg := newDefaultConfig()
	cfg.Apply(fCfg)
	cfg.Apply(pCfg)
	return cfg, nil
}
