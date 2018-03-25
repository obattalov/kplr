package main

import (
	"context"
	"os"
	"os/signal"
	"net/http"
	"net/url"
	"encoding/json"
	"log/syslog"
	"bytes"
	"io"
	"io/ioutil"
	"errors"
	"fmt"

	"github.com/jrivets/log4g"
)

const (
	cursorIDfield = "curID"
	defaultConfigFile = "config.json"
	)

type myHttpResponse http.Response

type (

	someValues interface{}

	iForwarder interface {
		NoSavedData()	bool
		ClearSavedData()
	}

	Config struct {
		KQL			string

		AgregatorIP	string

		RecieverIP	string
		LogPriority syslog.Priority
		LogTag		string
	}

	Forwarder struct {
		config 		*Config
		logger 		log4g.Logger
		curID		int64
		httpClient	*http.Client
		ctx 		context.Context
		ctxCancel 	context.CancelFunc
		savedData 	bytes.Buffer
		r 			io.Reader
		w 			io.Writer
	}

)

var FLogger = log4g.GetLogger("Forwarder")

func main() {
	configs, err := parseCLP()
	if err != nil {
		FLogger.Error("Could not parse config file. Err: ", err)
		return
	}

	if len(configs) == 0 {
		FLogger.Warn("No configurated forwarders")
		return
	}

	for _, cur_cfg := range configs { //goroutines for each config-record


		go func(cur_cfg *Config) {
			fwdr, err := NewForwarder(cur_cfg)
			if err != nil {
				FLogger.Error(fmt.Sprintf("Could not create forwarder %v. Err = %s", cur_cfg.LogTag, err))
				return
			}
			fwdr.logger.Info("Initializing forwarder " + cur_cfg.LogTag)
			defer fwdr.logger.Info("Forwarder " + cur_cfg.LogTag + " is over.")
		L1:
			for {
				select {
				case <-fwdr.ctx.Done():
					break L1
				default:
					fwdr.ForwardData()
				}
			}
		}(&cur_cfg)



	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	select {
	case <-signalChan:
		FLogger.Warn("Interrupt signal is received")
	}


}






func NewForwarder(cfg *Config) (*Forwarder , error) {

	fwdr := new (Forwarder)
	fwdr.config = cfg
	fwdr.logger = log4g.GetLogger("fwdr-" + cfg.LogTag)
	fwdr.httpClient = new(http.Client)
	fwdr.ctx, fwdr.ctxCancel = context.WithCancel(context.Background())

	rsysWriter, err := syslog.Dial("tcp", cfg.RecieverIP, cfg.LogPriority, cfg.LogTag) //rsyslog writer
	if err != nil {
		fwdr.logger.Info("Could not create r-sys-log writer. Error =", err)
		return nil, err
	}

	var curID int64 = 0 //gettig cursor id from key-value store or config-file
	if curID == 0 {
		uv := url.Values{}
		uv.Set("",fwdr.config.KQL)
		resp, err := fwdr.httpClient.PostForm(fwdr.config.AgregatorIP + "/cursor", uv)
		if err != nil {
			fwdr.logger.Info("Could not get a new cursor. Error =", err)
			return nil, err
		}


		//func ReadAtLeast(r Reader, buf []byte, min int) (n int, err error)
		//func (c *Client) PostForm(url string, data url.Values) (resp *Response, err error)
		/*
		type Response struct {
			StatusCode int    // e.g. 200
			Body io.ReadCloser
			ContentLength int64
			}
		*/
		//func Unmarshal(data []byte, v interface{}) error

		resp_j := make(map[string]someValues)
		err = ResponseToJSON(resp, &resp_j)

		if err != nil {
			fwdr.logger.Info("Could not get JSON from agregator response. Error =", err)
			return nil, err			
		}

		curID, _ = resp_j[cursorIDfield].(int64) //type assertion
		fwdr.savedData.Reset() //put here remained logs data
	}
	fwdr.curID = curID
	fwdr.w = io.MultiWriter(&fwdr.savedData, rsysWriter)
	return fwdr, nil
}


func (fwdr *Forwarder) ForwardData() error {
	if fwdr.NoSavedData() {
	//if no saved data
		resp, err := fwdr.httpClient.Get(fwdr.config.AgregatorIP + "/cursors:" + string(fwdr.curID))
		if err != nil {
			fwdr.logger.Error("Error while getting data by /cursors:. Error = ", err)
			return err
		}
		_, err = io.Copy(fwdr.w, resp.Body)
		if err != nil {
			fwdr.logger.Error("Error while sending/saving data. Error = ", err)
			return err
		}
		fwdr.ClearSavedData();
		return nil
	}
	//if saved data exists
		//sending data
	if _, err := io.Copy(fwdr.w, &fwdr.savedData); err != nil {
		fwdr.logger.Error("Error while sending data. Error = ", err)			
		return err
	}
	fwdr.ClearSavedData()
	return nil
}

func (fwdr *Forwarder) ClearSavedData() {
	fwdr.savedData.Reset()
}

func (fwdr *Forwarder) NoSavedData() bool {
	return fwdr.savedData.Len() == 0
}


func ResponseToJSON(resp *http.Response, jresp interface{}) error {
	buf := make ([]byte, resp.ContentLength)
	_, _ = io.ReadFull(resp.Body, buf)
	err := json.Unmarshal(buf, jresp)
	return err
}

func parseCLP() ([]Config, error) {
	var filename = defaultConfigFile
	if IsFileNotExist(filename) {
		return nil, errors.New("No forwarders config file" + filename)
	}

	cfgData, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Could not read configuration file %v. Err = %s", filename, err))
	}

	var (
		cfg []Config
		cfg_arr []someValues
		)
	err = json.Unmarshal(cfgData, cfg_arr)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Could not unmarshal data from %v. Err = %s", filename, err))
	}
	for _, cfgi := range cfg_arr {
		cfgc, ok := cfgi.(Config)
		if !ok {
			return nil, errors.New(fmt.Sprintf("Incorrect data in forwarders configuration file %v. Err = %s", filename, err))
		}
		cfg = append(cfg, cfgc)
	}
	FLogger.Info("Configuration read from ", filename)

	return cfg, nil
}

func IsFileNotExist(filename string) bool {
	_, err := os.Stat(filename)
	return os.IsNotExist(err)
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