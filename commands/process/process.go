package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	process "cs425/mp/process"
)

var (
	log_process_port  = flag.Int("log_process_port", 50052, "The logger process port")
	devmode           = flag.Bool("devmode", false, "Develop locally?")
	logtofile         = false
	introducerAddress = "shahidi3@fa22-cs425-3701.cs.illinois.edu"
	introducerPort    = 50053
)

func main() {
	port := flag.Int("port", 50054, "The failure detector process port")
	flag.Parse()
	log.Printf("port: %v", *port)
	wg := new(sync.WaitGroup)
	wg.Add(3)
	if logtofile {
		// write logs of the service process to service.log file
		f, err := os.OpenFile("process.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("error opening file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	if *devmode {
		introducerAddress = "localhost"
	}

	// go process.StartLogServer(*log_process_port, wg)

	go process.JoinNetwork(fmt.Sprintf("%s:%d", introducerAddress, introducerPort), *port, wg)

	// Wait for the wait group to be done
	wg.Wait()
}
