package main

import (
"fmt"
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


func main() {

	var cfg []Config

	var cfg_arr = someValues{
		AgregatorIP	:	"localhost:8080",

		RecieverIP	:	"localhost:5000",

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
	fmt.Printf("%s", cfg)
	return
}