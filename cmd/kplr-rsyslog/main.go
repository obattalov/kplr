package main

import (
"io"
"net/http"
"log/syslog"
"os"
"os/signal"
"gopkg.in/alecthomas/kingpin.v2"
"strings"
"github.com/jrivets/log4g"
)

type kplrReader struct {
	R io.Reader
}

var	logger = log4g.GetLogger("fwdr")

func main() {
	var (
		RecieverIP = kingpin.Flag("rsyslogip", "rsyslog server ip:port").Short('r').Default("127.0.0.1:514").String()
		AgregatorIP = kingpin.Flag("agregatorip","kepler agregator ip:port").Default("127.0.0.1:8080").Short('a').String()
		JournalList = kingpin.Flag("journals","list of forwarded journals: jrnl1,jrnl2...").Short('j').Default("auth.log").String()
		LogTag = kingpin.Flag("logtag","logtag of rsyslog server event").Default("").Short('t').String()
		LogPriority = kingpin.Flag("logpriority", "logpriority of rsyslog server event").Short('p').Default("0").Int()
		raw_query string
		)

	kingpin.Parse()
	*AgregatorIP = "http://" + *AgregatorIP

	rsysWriter, err := syslog.Dial("tcp", *RecieverIP, syslog.Priority(*LogPriority), *LogTag) //rsyslog writer
	if err != nil {
		logger.Error("Could not dial r-sys-log server (%v). Error = %s\n", *RecieverIP, err)
		return
	}

	journals := strings.Split(*JournalList, ",")
	for _, v := range journals {
		raw_query += "from=" + v + ""
	}

	resp, err := http.Get(*AgregatorIP + "/logs?" + raw_query)
	if err != nil {
		logger.Error("Unable to prepare http request. Error = %s\n", err)
		return		
	}

	if err != nil {
		logger.Error("Could not get request. Error = %s\n", err)
		return				
	}
	defer resp.Body.Close()

	w := io.MultiWriter(rsysWriter) //, os.Stdout)

	r := kplrReader{resp.Body}


	go func(){
		_, err := io.Copy(w, &r)
		if err != nil {
			logger.Error("Error while copying = %s\n", err)
			return				
		}
		}()

		signalChan := make(chan os.Signal, 1)
		signal.Notify(signalChan, os.Interrupt)
		<-signalChan
		logger.Info("Interrupt signal is received")
	}

	func (r *kplrReader) Read (b []byte) (int, error) {
		var (
			ln int
			fs string
			)
		
		buf := make([]byte, 8096)
		n, err := r.R.Read(buf)
		if err != nil {
			logger.Error("Error while reading in read: ", err)
		}
		logger.Info("==Got ", n, " bytes") 
		strBytes := strings.Split(string(buf[:n-1]), "\n")
		for _, ls := range strBytes {
			ln += len(ls)
			ls = ls[16:] + "\n"
			fs += ls
			logger.Info(">> ", ls)
		}
		copy(b, []byte(fs))
		return len(fs)-1, nil
	}