package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/jrivets/log4g"
	"github.com/kplr-io/kplr"
)

func main() {
	cfg, err := parseCLP()
	if err != nil {
		return
	}
	defer log4g.Shutdown()

	if cfg == nil {
		return
	}

	kplr.DefaultLogger.Info("Kepler is starting...")
	injector := inject.NewInjector(log4g.GetLogger("kplr.injector"), log4g.GetLogger("fb.injector"))

	mainCtx, cancel := context.WithCancel(context.Background())
	defer kplr.DefaultLogger.Info("Exiting. kplr main context is shutdown.")
	defer injector.Shutdown()
	defer cancel()

	fwdr, err := forwarder.NewAgregator(&forwarder.Config{
		IP: cfg.AgregatorIP,
		JournalName: cfg.JournalName,
		CurStartPos: curStartPos})

	injector.RegisterOne(fwdr, "fwdr")

	injector.Construct()	

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	//parametres:
	//- config filename (default config.json)
	//reading config:
	//- access to agregator
	//- rsyslog sending parametres
	//- the name of last read record key in kubernetes
	//getting a number of last read record from kubernetes
	//creating a channel

	<-signalChan // wait signals
	log.Printf("Shutting down...")

	close(stop) // stop gorutines
	wg.Wait()   // wait until everything's stopped

}

///// work with io http
/*
package main

import (
	"net/http"
//	"bytes"
	"fmt"
	"encoding/json"
	"net/url"
	"io"
	
)

type myHttpResponse http.Response

type (
	myValues interface{}
)
*/
/*
//workable
func main() {
	var buf bytes.Buffer
	hc := &http.Client{}
	resp, _ := hc.Get("http://localhost:8080/journals")
	n, _ := buf.ReadFrom(resp.Body)
	fmt.Printf("Read %v bytes\n", n)
	fmt.Println(buf.String())

	jresp := make(map[string]myValues)
	bts := make([]byte, n)
	n1, _ := buf.Read(bts)
	fmt.Printf("n = %v\n", n1)
	err := json.Unmarshal(bts, &jresp)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("jresp = ", jresp)
	for k, v := range jresp {
		fmt.Printf("%v = %v\n", k, v)
	}


}
*/
/*
func main() {
	hc := &http.Client{}
	data := url.Values{}
	data.Set("__source_id__", "dkpg.log")
	jresp := make(map[string]myValues)

	resp, _ := hc.PostForm("http://localhost:8080/cursors",data)
	ToJSON(resp, &jresp)

	for k, v := range jresp {
		fmt.Printf("%v = %v\n", k, v)
	}


}

func ResponseToJSON(resp *http.Response, jresp *map[string]myValues) {
	buf := make ([]byte, resp.ContentLength)
	_, _ = io.ReadFull(resp.Body, buf)
	_ = json.Unmarshal(buf, jresp)

}
*/