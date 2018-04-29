package main

import (
"fmt"
"io"
"net/http"
"log/syslog"
"os"
"os/signal"
"gopkg.in/alecthomas/kingpin.v2"
"strings"
"bufio"
)

type kql_json struct {
	Contenttype string `json:"Content-type"`
	From []string `json:"from"`
}



func main() {
	var (
		RecieverIP = kingpin.Flag("rsyslogip", "rsyslog server ip:port").Short('r').Default("127.0.0.1:5000").String()
		AgregatorIP = kingpin.Flag("agregatorip","kepler agregator ip:port").Default("http://127.0.0.1:8080").Short('a').String()
		JournalList = kingpin.Flag("journals","list of forwarded journals: jrnl1,jrnl2...").Short('j').Required().String()
		LogTag = kingpin.Flag("logtag","logtag of rsyslog server event").Default("").Short('t').String()
		LogPriority = kingpin.Flag("logpriority", "logpriority of rsyslog server event").Short('p').Int()
//		ConfigFile = kingpin.Flag("config","config file path").Short('f').ExistingFile()
		raw_query string
		)

	kingpin.Parse()

	//*RecieverIP = "127.0.0.1:5000"

	rsysWriter, err := syslog.Dial("tcp", *RecieverIP, syslog.Priority(*LogPriority), *LogTag) //rsyslog writer
	if err != nil {
		fmt.Printf("Could not dial r-sys-log server (%v). Error = %s\n", *RecieverIP, err)
		return
	}

	journals := strings.Split(*JournalList, ",")
	for _, v := range journals {
		raw_query += "from=" + v + ""
	}

	resp, err := http.Get(*AgregatorIP + "/logs?" + raw_query)
	if err != nil {
		fmt.Printf("Unable to prepare http request. Error = %s\n", err)
		return		
	}

	if err != nil {
		fmt.Printf("Could not get request. Error = %s\n", err)
		return				
	}
	defer resp.Body.Close()

	w := io.MultiWriter(rsysWriter, os.Stdout)
	r := bufio.NewReader(resp.Body)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
//	for {
		select {
		case <-signalChan:
			fmt.Println("Interrupt signal is received")
		default:
			fmt.Println("cycle")
			n, err := io.Copy(w, r)
			if err != nil {
				fmt.Printf("Error while copying = %s\n", err)
				return				
			}
			fmt.Printf("Copied %v bytes\n", n)
		}
//	}



}




/*


curl -XPOST -d'{"from":["auth.log"],"blocking":false}' -H "Content-type:application/json" http://127.0.0.1:8080/cursors

curl -XGET -d'{"from":["auth.log"],"blocking":true}' -H "Content-type:application/json" http://127.0.0.1:8080/logs


*/