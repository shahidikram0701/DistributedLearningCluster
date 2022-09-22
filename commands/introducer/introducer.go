package main

import (
	"flag"
	"log"
	"os"
	"sync"

	intro "cs425/mp/introducer"
)

var (
	port      = flag.Int("port", 50053, "The port where the introducer runs")
	devmode   = flag.Bool("devmode", false, "Develop locally?")
	logtofile = false
)

func main() {
	wg := new(sync.WaitGroup)

	wg.Add(1)
	flag.Parse()

	if logtofile {
		// write logs of the service process to introducer.log file
		f, err := os.OpenFile("introducer.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("error opening file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	// Start the introducer
	intro.StartIntroducerAndListenToConnections(*devmode, *port, wg)

	wg.Wait()
}
