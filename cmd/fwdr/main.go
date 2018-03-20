package main

import (
	"config"
)

func main() {
	//parametres:
	//- config filename (default config.json)
	//reading config:
	//- access to agregator
	//- rsyslog sending parametres
	//- the name of last read record key in kubernetes
	//getting a number of last read record from kubernetes

	//starting gorutine getRecords
	go func(config) {
		//infinit loop
			//getting records, cursor_id
			//calculate last record number
			//put records and last number into channel




		//end of infinit loop
	}


	//starting gorutine sendRecords
	go func(config) {
		//infinit loop
			//getting records and last record number from channel
			//loop while not success
				//sending to reciever
			//save last record number in kubernetes key
		//end of infinit loop
	}

}