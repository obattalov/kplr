package main

import (
	"context"
//	"strconv"
//	"os"
//	"os/signal"
	"bytes"
	"net/http"
	"net/url"
	"encoding/json"
	"github.com/jrivets/log4g"
	"io"
	"log/syslog"
)

const cursorIDfield = "curID"

type myHttpResponse http.Response

type (

	someValues interface{}

	iForwarder interface {
		NoSavedData()	bool
		ClearSavedData()
	}

	Config struct {
		KQL			string

		ForwarderID	string
		AgregatorIP	string

		RecieverID	string
		RecieverIP	string
		LogPriority syslog.Priority
		LogTag		string
	}

	Forwarder struct {
		i 			iForwarder
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

func NewForwarder(cfg *Config) (*Forwarder , error) {

	fwdr := new (Forwarder)
	fwdr.config = cfg
	fwdr.logger = log4g.GetLogger("fwdr")
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

func (fwdr *Forwarder) DiInit() error {
	fwdr.logger.Info("Initializing.")



	go func() {
		defer fwdr.logger.Info("Sweeper goroutine is over.")
	L1:
		for {
			select {
			case <-fwdr.ctx.Done():
				break L1
			default:
				fwdr.ForwardData()
			}
		}
	}()

	return nil
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
