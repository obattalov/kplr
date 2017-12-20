package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/kplr/model"
	"github.com/kplr-io/kplr/model/wire"
	"github.com/kplr-io/zebra"
)

type (
	conn struct {
		logger     log4g.Logger
		zcl        zebra.Writer
		maxLines   int
		jrnlId     string
		tagVal     string
		lines      []string
		pktWriter  *wire.Writer
		pktEncoder *model.SimpleLogEventEncoder
	}
)

func main() {
	var (
		help     bool
		kplrAddr string
		jrnlId   string
		tagVal   string
		packSize int
		fileName string
	)

	flag.StringVar(&kplrAddr, "kplr-addr", "127.0.0.1:9966", "kplr address")
	flag.StringVar(&jrnlId, "jid", "test-jrnl", "the journal identifier where data will be written to")
	flag.StringVar(&fileName, "src", "", "the source of records")
	flag.StringVar(&tagVal, "tag", "test", "tag for all records")
	flag.IntVar(&packSize, "pk-size", 1000, "maximum size of the package to be written")
	flag.BoolVar(&help, "help", false, "prints the usage")

	flag.Parse()

	if help {
		fmt.Fprintf(os.Stderr, "%s is kplr daemon which serves Log Aggregator requests\n", os.Args[0])
		flag.Usage()
		return
	}

	log := log4g.GetLogger("kplr-ingetsor")
	defer log4g.Shutdown()

	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		log.Warn("The file ", fileName, " is not found.")
		return
	}

	f, err := os.Open(fileName)
	if err != nil {
		log.Error("Could not open the file ", fileName, " for read, err=", err)
		return
	}
	defer f.Close()

	cn, err := newConn(kplrAddr, packSize, jrnlId, tagVal)
	if err != nil {
		log.Error("Could not create connection to kplr, err=", err)
		return
	}
	defer cn.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		cn.newLine(scanner.Text() + "\n")
	}

	if err := scanner.Err(); err != nil {
		log.Error("Some error while file scanning err=", err)
		return
	}
	cn.flush()
}

func newConn(addr string, maxLines int, jrnlId, tagVal string) (*conn, error) {
	zcl, err := zebra.NewTcpClient(addr, &zebra.ClientConfig{AccessKey: ""})
	if err != nil {
		return nil, err
	}
	c := new(conn)
	c.zcl = zcl
	c.tagVal = tagVal
	c.maxLines = maxLines
	c.jrnlId = jrnlId
	c.lines = make([]string, 0, maxLines)
	c.pktEncoder = &model.SimpleLogEventEncoder{}
	c.pktWriter = wire.NewWriter(c.pktEncoder)
	c.logger = log4g.GetLogger("conn")
	c.logger.Info("Will connct to ", addr, " writing by ", maxLines, " lines per packet. jrnlId=", jrnlId, " tag=", tagVal)
	return c, nil
}

func (c *conn) newLine(line string) {
	c.lines = append(c.lines, line)
	if len(c.lines) == cap(c.lines) {
		c.flush()
	}
}

func (c *conn) flush() {
	if len(c.lines) == 0 {
		return
	}
	defer func() { c.lines = c.lines[:0] }()

	// Creating header and group tags
	header := model.SSlice{model.TAG_SRC_ID, model.WeakString(c.jrnlId), "tag", model.WeakString(c.tagVal)}
	tags := model.TagSubst("src_id", c.jrnlId)
	tags = tags.Add("tag", c.tagVal)
	c.pktEncoder.SetTags(tags)
	buf, err := c.pktWriter.MakeBtsBuf(header, c.lines)
	if err != nil {
		c.logger.Error("Could not make packet for ", len(c.lines), ", lines, err=", err)
		return
	}

	_, err = c.zcl.Write(buf, nil)
	if err != nil {
		c.logger.Error("Could not send packet of ", len(buf), " bytes length for ", len(c.lines), ", lines, err=", err)
	}
}

func (c *conn) Close() error {
	c.flush()
	return c.zcl.Close()
}
