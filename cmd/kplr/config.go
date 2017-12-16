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
		ListenOn          string
		SessionTimeoutSec *int `json:"SessionTimeoutSec,omitempty"`
		JournalsDir       string

		HttpListenOn    string
		HttpShtdwnToSec int
		HttpDebugMode   bool
	}
)

var configLog = log4g.GetLogger("kplr.Config")

const (
	defaultKeplerDir         = "/opt/kplr/"
	defaultKplrJrnlsDir      = defaultKeplerDir + "journals/"
	defaultConfigFile        = defaultKeplerDir + "config.json"
	defaultLog4gCongigFile   = defaultKeplerDir + "log4g.properties"
	defaultZebraPort         = ":9966"
	defaultSessTimeoutSec    = 30
	defaultHttpAddr          = ":8080"
	defaultHttpShutdownToSec = 5
)

func newDefaultConfig() *Config {
	cfg := &Config{}
	cfg.ListenOn = defaultZebraPort
	cfg.SessionTimeoutSec = kplr.GetIntPtr(defaultSessTimeoutSec)
	cfg.JournalsDir = defaultKplrJrnlsDir
	cfg.HttpListenOn = defaultHttpAddr
	cfg.HttpShtdwnToSec = defaultHttpShutdownToSec
	cfg.HttpDebugMode = false
	return cfg
}

func (c *Config) String() string {
	return fmt.Sprint(
		"\n\tListenOn=", c.ListenOn,
		"\n\tSessionTimeoutSec=", kplr.GetIntVal(c.SessionTimeoutSec, -1),
		"\n\tJournalsDir=", c.JournalsDir,
		"\n\tHttpListenOn=", c.HttpListenOn,
		"\n\tHttpShtdwnToSec=", c.HttpShtdwnToSec,
		"\n\tHttpDebugMode=", c.HttpDebugMode,
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

func (c *Config) Apply(c2 *Config) {
	if c2.ListenOn != "" {
		c.ListenOn = c2.ListenOn
	}
	if c2.SessionTimeoutSec != nil {
		c.SessionTimeoutSec = c2.SessionTimeoutSec
	}
	if c2.JournalsDir != "" {
		c.JournalsDir = c2.JournalsDir
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
