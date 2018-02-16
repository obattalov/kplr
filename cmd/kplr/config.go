package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/kplr"
)

type (
	Config struct {
		JournalsDir string

		// Zebra (logs feed) endpoint
		ZebraListenOn          string
		ZebraSessionTimeoutSec *int `json:"SessionTimeoutSec,omitempty"`
		ZebraKeyFN             string
		ZebraCertFN            string
		ZebraCaFN              string
		Zebra2WayTls           bool

		HttpListenOn    string
		HttpShtdwnToSec int
		HttpDebugMode   bool
		HttpsKeyFN      string
		HttpsCertFN     string

		JrnlChunkMaxSize int64
		JrnlMaxSize      int64
	}
)

var configLog = log4g.GetLogger("kplr.Config")

const (
	defaultKeplerDir           = "/opt/kplr/"
	defaultKplrJrnlsDir        = defaultKeplerDir + "journals/"
	defaultConfigFile          = defaultKeplerDir + "config.json"
	defaultLog4gCongigFile     = defaultKeplerDir + "log4g.properties"
	defaultZebraPort           = "0.0.0.0:9966"
	defaultZebraSessTimeoutSec = 30
	defaultHttpAddr            = ":8080"
	defaultHttpShutdownToSec   = 5
	defaultJrnChunkMaxSize     = int64(50000000)      // 50Mb
	defaultJrnlMaxSize         = int64(1000000000000) // 1Tb
)

func newDefaultConfig() *Config {
	cfg := &Config{}
	cfg.ZebraListenOn = defaultZebraPort
	cfg.ZebraSessionTimeoutSec = kplr.GetIntPtr(defaultZebraSessTimeoutSec)
	cfg.JournalsDir = defaultKplrJrnlsDir
	cfg.HttpListenOn = defaultHttpAddr
	cfg.HttpShtdwnToSec = defaultHttpShutdownToSec
	cfg.HttpDebugMode = false
	cfg.JrnlChunkMaxSize = defaultJrnChunkMaxSize
	cfg.JrnlMaxSize = defaultJrnlMaxSize
	return cfg
}

func (c *Config) String() string {
	return fmt.Sprint(
		"\n\tZebraListenOn=", c.ZebraListenOn,
		"\n\tZebraKeyFN=", c.ZebraKeyFN,
		"\n\tZebraCertFN=", c.ZebraCertFN,
		"\n\tZebraCaFN=", c.ZebraCaFN,
		"\n\tZebra2WayTls=", c.Zebra2WayTls,
		"\n\tZebraSessionTimeoutSec=", kplr.GetIntVal(c.ZebraSessionTimeoutSec, -1),
		"\n\tJournalsDir=", c.JournalsDir,
		"\n\tHttpListenOn=", c.HttpListenOn,
		"\n\tHttpShtdwnToSec=", c.HttpShtdwnToSec,
		"\n\tHttpDebugMode=", c.HttpDebugMode,
		"\n\tHttpsKeyFN=", c.HttpsKeyFN,
		"\n\tHttpsCertFN=", c.HttpsCertFN,
		"\n\tJrnlChunkMaxSize=", kplr.FormatSize(c.JrnlChunkMaxSize),
		"\n\tJrnlMaxSize=", kplr.FormatSize(c.JrnlMaxSize),
	)
}

// ============================ RestApiConfig ================================
func (c *Config) GetHttpAddress() string {
	return c.HttpListenOn
}

func (c *Config) GetHttpShtdwnTOSec() int {
	return c.HttpShtdwnToSec
}

func (c *Config) IsHttpDebugMode() bool {
	return c.HttpDebugMode
}

func (c *Config) GetHttpsCertFile() string {
	return c.HttpsCertFN
}

func (c *Config) GetHttpsKeyFile() string {
	return c.HttpsKeyFN
}

// ============================ JournalConfig ================================
func (c *Config) GetJournalDir() string {
	return c.JournalsDir
}

func (c *Config) GetJournalChunkSize() int64 {
	return c.JrnlChunkMaxSize
}

func (c *Config) GetJournalMaxSize() int64 {
	return c.JrnlMaxSize
}

// =============================== Config ====================================
func (c *Config) Apply(c2 *Config) {
	if c2.ZebraListenOn != "" {
		c.ZebraListenOn = c2.ZebraListenOn
	}
	if c2.ZebraKeyFN != "" {
		c.ZebraKeyFN = c2.ZebraKeyFN
	}
	if c2.ZebraCertFN != "" {
		c.ZebraCertFN = c2.ZebraCertFN
	}
	if c2.ZebraCaFN != "" {
		c.ZebraCaFN = c2.ZebraCaFN
	}
	if c2.Zebra2WayTls {
		c.Zebra2WayTls = c2.Zebra2WayTls
	}
	if c2.ZebraSessionTimeoutSec != nil {
		c.ZebraSessionTimeoutSec = c2.ZebraSessionTimeoutSec
	}
	if c2.JournalsDir != "" {
		c.JournalsDir = c2.JournalsDir
	}
	if c2.JrnlMaxSize > 0 {
		c.JrnlMaxSize = c2.JrnlMaxSize
	}
	if c2.JrnlChunkMaxSize > 0 {
		c.JrnlChunkMaxSize = c2.JrnlChunkMaxSize
	}
	if c2.HttpDebugMode {
		c.HttpDebugMode = c2.HttpDebugMode
	}
	if c2.HttpListenOn != "" {
		c.HttpListenOn = c2.HttpListenOn
	}
	if c2.HttpShtdwnToSec > 0 {
		c.HttpShtdwnToSec = c2.HttpShtdwnToSec
	}
	if c2.HttpsKeyFN != "" {
		c.HttpsKeyFN = c2.HttpsKeyFN
	}
	if c2.HttpsCertFN != "" {
		c.HttpsCertFN = c2.HttpsCertFN
	}
}

func (c *Config) readFromFile(filename string) {
	if filename == "" {
		return
	}

	if kplr.IsFileNotExist(filename) {
		configLog.Warn("There is no file ", filename, " for reading kplr config, will use default configuration.")
		return
	}

	cfgData, err := ioutil.ReadFile(filename)
	if err != nil {
		configLog.Fatal("Could not read configuration file ", filename, ": ", err)
		panic(err)
	}

	cfg := &Config{}
	err = json.Unmarshal(cfgData, cfg)
	if err != nil {
		configLog.Warn("Could not unmarshal data from ", filename, ", err=", err)
		return
	}
	configLog.Info("Configuration read from ", filename)
	c.Apply(cfg)
}
