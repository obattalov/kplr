package main

import (
	"github.com/obattalov/kplr/cmd/fwdr/forwarder"
	"github.com/obattalov/kplr/cmd/fwdr/config"
	"net/http"
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
		IP: cfg.AgregatorIP
		JournalName: cfg.JournalName
		CurStartPos: curStartPos
		})

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