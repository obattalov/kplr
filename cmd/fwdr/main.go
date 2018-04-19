package main

import (
"fmt"
"io"
"net/http"
"bytes"
"log/syslog"
"os"
"os/signal"
)

type (

	someValues struct {
		AgregatorIP	string
		RecieverIP	string
		Journals 	[]JDesc

	}

	iForwarder interface {
		NoSavedData()	bool
		ClearSavedData()
	}

	JDesc struct {
		Journal		string
		LogPriority int
		LogTag		string
	}

	Config struct {
		Journal		string

		AgregatorIP	string

		RecieverIP	string
		LogPriority int
		LogTag		string
	}

)


func main1() {

	var cfg []Config

	var cfg_arr = someValues{
		AgregatorIP	:	"127.0.0.1:8080",

		RecieverIP	:	"127.0.0.1:5000",

		Journals	: 	[]JDesc{{	
				Journal:		"dpkg.log",

		LogPriority: 	100,
		LogTag:		"dpkg"},
		{			Journal:		"krnl.log",

		LogPriority: 	105,
		LogTag:		"krnl"},
		{				Journal:		"auth.log",

		LogPriority: 	110,
		LogTag:		"auth"}}}


	for _, cfgi := range cfg_arr.Journals {
		var cfgc Config

		if cfgi.Journal == "" {
			fmt.Printf("Error in Journal description: no Journal name")
			return
		}
		if cfgi.LogPriority == 0 {
			fmt.Printf("Error in Journal description: no LogPriority (int)")
			return
		}
		if cfgi.LogTag == "" {
			fmt.Printf("Error in Journal description: no LogTag value")
			return
		}
		cfgc.Journal = cfgi.Journal
		cfgc.LogPriority = cfgi.LogPriority
		cfgc.LogTag = cfgi.LogTag		
		cfgc.RecieverIP 	= cfg_arr.RecieverIP
		cfgc.AgregatorIP 	= cfg_arr.AgregatorIP
		cfg = append(cfg, cfgc)
	}
	//fmt.Printf("%s", cfg)
	return
}

func main() {
	var (
		savedData bytes.Buffer
		RecieverIP = "127.0.0.1:5000"
		AgregatorIP = "http://127.0.0.1:8080"
		)

	rsysWriter, err := syslog.Dial("tcp", RecieverIP, 100, "test_log_tag") //rsyslog writer
	if err != nil {
		fmt.Printf("Could not create r-sys-log writer. Error = %s\n", err)
		return
	}

	KQL := "select from kern.log position tail offset 5 limit -1"
	w := io.MultiWriter(&savedData, rsysWriter, os.Stdout)

	get_req := AgregatorIP + "/logs?kql=" + string(KQL)
	fmt.Printf("Trying to get %s\n", get_req)
	resp, err := http.Get(get_req)
	if err != nil {
		fmt.Printf("Could not get request. Error = %s\n", err)
		return				
	}
	defer resp.Body.Close()
	go func(r *http.Response) {
//		for {
			fmt.Println("cycle")
			n, err := io.Copy(w, r.Body)
			if err != nil {
				fmt.Printf("Error while copying = %s\n", err)
				return				
			}
			fmt.Printf("Copied %v bytes\n", n)
//		}
	}(resp)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	select {
	case <-signalChan:
		fmt.Println("Interrupt signal is received")
	}

}

/*

curl -XGET -d 'select "\\{\"msg\": {{msg}}\\}\n" from "system.log" limit 10'  localhost:8080/logs
т.е. можно писать так 
select <fmt string> from <journals> were ... limit...
fmt string это строчка, в которой переменные в {}

*/