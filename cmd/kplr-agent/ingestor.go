package main

import (
	"fmt"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/geyser"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/wire"
	"github.com/kplr-io/zebra"
)

type (
	IngestorConfig struct {
		Server           string `json:"server"`
		SourceId         string `json:"sourceId"`
		PacketMaxRecords int    `json:"packetMaxRecords"`
		AccessKey        string `json:"accessKey"`
		SecretKey        string `json:"secretKey"`
	}

	Ingestor struct {
		cfg       *IngestorConfig
		zClient   zebra.Writer
		pktWriter *wire.Writer
		logger    log4g.Logger
	}
)

//=== ingestor

func newIngestor(cfg *IngestorConfig, headerModel model.Meta) (*Ingestor, error) {
	if err := checkConfig(cfg); err != nil {
		return nil, err
	}

	logger := log4g.GetLogger("kplr.ingestor")
	logger.Info("Creating, config=", geyser.ToJsonStr(cfg))

	zcl, err := zebra.NewTcpClient(cfg.Server,
		&zebra.ClientConfig{AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey})
	if err != nil {
		return nil, err
	}
	ing := &Ingestor{
		cfg:       cfg,
		zClient:   zcl,
		pktWriter: wire.NewWriter(&model.SimpleLogEventEncoder{}, headerModel),
		logger:    logger,
	}

	logger.Info("Created!")
	return ing, nil
}

func (i *Ingestor) Ingest(header model.Event, lines []string) error {
	buf, err := i.pktWriter.MakeBtsBuf(header, lines)
	if err != nil {
		return err
	}
	_, err = i.zClient.Write(buf, nil)
	if err != nil {
		return err
	}
	return nil
}

func (i *Ingestor) Close() {
	i.logger.Info("Closing...")
	if i.zClient != nil {
		i.zClient.Close()
	}
	i.logger.Info("Closed.")
}

//=== helpers

func checkConfig(cfg *IngestorConfig) error {
	if cfg == nil {
		return fmt.Errorf("invalid config=%v", cfg)
	}
	if cfg.Server == "" {
		return fmt.Errorf("invalid config; server=%v, must be non-empty", cfg.Server)
	}
	if cfg.SourceId == "" {
		return fmt.Errorf("invalid config; sourceId=%v, must be non-empty", cfg.SourceId)
	}
	if cfg.PacketMaxRecords <= 0 {
		return fmt.Errorf("invalid config; packetMaxRecords=%v, must be > 0", cfg.PacketMaxRecords)
	}
	return nil
}
