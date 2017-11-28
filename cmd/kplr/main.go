package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/kplr"
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

	_, cancel := context.WithCancel(context.Background())
	defer kplr.DefaultLogger.Info("Exiting. kplr main context is shutdown.")
	defer cancel()

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
