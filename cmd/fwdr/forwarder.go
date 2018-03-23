package main

import (
	"context"
//	"os"
//	"os/signal"
	"bytes"
	"net/http"
	"encoding/json"
	"github.com/jrivets/log4g"
	"io"
	"log/syslog"
)

type (
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
		LogPriority int
		LogTag		string
	}

	Forwarder struct {
		i 			iForwarder
		config 		*Config
		logger 		log4g.Logger
		curID		uint64
		httpClient	http.Client
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
	fwdr.httpClient = &http.Client{}
	fwdr.ctx, fwdr.ctxCancel = context.WithCancel(context.Background())

	rsysWriter, err := syslog.Dial("tcp", cfg.RecieverIP, fwdr.LogPriority, fwdr.LogTag) //rsyslog writer
	if err != nil {
		fwdr.logger.Info("Could not create r-sys-log writer. Error =", err)
		return nil, err
	}

	curID := nil //gettig cursor id from key-value store or config-file
	if curID == nil {
		resp, err := fwdr.httpClient.PostForm(fwdr.config.IP + "/cursor", nil)
		if err != nil {
			fwdr.logger.Info("Could not get a new cursor. Error =", err)
			return nil, err
		}
		err = json.Unmarshal(resp, &resp_j)
		if err != nil {
			fwdr.logger.Info("Could not unmarshal cursorID from agregator response. Error =", err)
			return nil, err
		}
		curID = resp_j[0].value //


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


}

func (fwdr *Forwarder) ForwardData() error {
	if fwdr.NoSavedData() {
	//if no saved data
		resp, err := fwdr.httpClient.Get(fwdr.config.IP + "/cursors:" + fwdr.curID)
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

func (fwdr *Forwarder) NoSavedData() {
	return fwdr.savedData.Len() == 0
}

func (r *http.Response) Read(p []byte) (n int, err error) {
	n, err = r.Read(p)
	if err != nil {
		n = 0
		return
	}
	jresp = json.Unmarshal(string(p[:]))
	

}
