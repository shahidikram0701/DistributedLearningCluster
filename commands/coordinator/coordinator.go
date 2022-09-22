package main

import (
	"flag"
	"log"
	"os"
	"sync"

	coordinator "cs425/mp/coordinator"
)

var (
	devmode   = flag.Bool("devmode", false, "Develop locally?")
	port      = flag.Int("port", 50051, "The server port")
	logtofile = false
)

func main() {
	flag.Parse()
	wg := new(sync.WaitGroup)
	wg.Add(1)
	if logtofile {
		// configuring the log to be emitted to a log file
		f, err := os.OpenFile("coordinator.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("error opening file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	go coordinator.StartCoordinatorService(*port, *devmode, wg)

	// Wait for the wait group to be done
	wg.Wait()
}
