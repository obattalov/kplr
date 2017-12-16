package main

import (
	"fmt"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/geyser"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/model/k8s"
	"github.com/kplr-io/kplr/wire"
	"github.com/kplr-io/zebra"
)

type (
	ingestorConfig struct {
		Server           string        `json:"server"`
		SourceId         string        `json:"sourceId"`
		PacketMaxRecords int           `json:"packetMaxRecords"`
		MetaDataModel    metaDataModel `json:"metaDataModel"`
		AccessKey        string        `json:"accessKey"`
		SecretKey        string        `json:"secretKey"`
	}

	ingestor struct {
		cfg       *ingestorConfig
		zClient   zebra.Writer
		pktWriter *wire.Writer
		logger    log4g.Logger
	}

	metaDataModel string
)

const (
	k8sMetaModel metaDataModel = "k8s"
)

//=== ingestor

func newIngestor(cfg *ingestorConfig) (*ingestor, error) {
	if err := checkConfig(cfg); err != nil {
		return nil, err
	}

	logger := log4g.GetLogger("kplr.ingestor")
	logger.Info("Creating, config=", geyser.ToJsonStr(cfg))

	mm := getMetaDesc(cfg.MetaDataModel)
	zcl, err := zebra.NewTcpClient(cfg.Server,
		&zebra.ClientConfig{AccessKey: cfg.AccessKey, SecretKey: cfg.SecretKey})
	if err != nil {
		return nil, err
	}

	logger.Info("Created!")
	return &ingestor{
		cfg:       cfg,
		zClient:   zcl,
		pktWriter: wire.NewWriter(&model.SimpleLogEventEncoder{}, mm),
		logger:    logger,
	}, nil
}

func (i *ingestor) ingest(ev *geyser.Event) error {
	buf, err := i.pktWriter.MakeBtsBuf(getMetaData(i.cfg, ev), getLines(ev))
	if err != nil {
		return err
	}
	_, err = i.zClient.Write(buf, nil)
	if err != nil {
		return err
	}
	return nil
}

func (i *ingestor) reInit() error {
	var err error
	if i.zClient != nil {
		i.zClient.Close()
	}
	i.zClient, err = zebra.NewTcpClient(i.cfg.Server,
		&zebra.ClientConfig{AccessKey: i.cfg.AccessKey, SecretKey: i.cfg.SecretKey})
	return err
}

func (i *ingestor) close() {
	i.logger.Info("Closing...")
	if i.zClient != nil {
		i.zClient.Close()
	}
	i.logger.Info("Closed.")
}

//=== helpers

func getMetaDesc(mm metaDataModel) model.Meta {
	switch mm {
	case k8sMetaModel:
		return k8s.MetaDesc
	default:
		return nil
	}
}

func getMetaData(cfg *ingestorConfig, ev *geyser.Event) model.Event {
	switch cfg.MetaDataModel {
	case k8sMetaModel:
		var egm k8s.EgMeta
		egm.SrcId = cfg.SourceId
		//TODO: parse file to get other fields (contId, podId...)
		return egm.Event()
	default:
		return nil
	}
}

func getLines(ev *geyser.Event) []string {
	var lines []string
	for _, r := range ev.Records.Data {
		lines = append(lines, string(r))
	}
	return lines
}

func checkConfig(cfg *ingestorConfig) error {
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
	if getMetaDesc(cfg.MetaDataModel) == nil {
		return fmt.Errorf("invalid config; no meta model found of type=%v", cfg.MetaDataModel)
	}
	return nil
}
