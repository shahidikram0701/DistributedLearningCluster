package main

import (
	"flag"
	"log"
	"os"
	"sync"

	intro "cs425/mp/introducer"
	process "cs425/mp/process"
)

var (
	port          = flag.Int("port", 50053, "The port where the introducer runs")
	devmode       = flag.Bool("devmode", false, "Develop locally?")
	udpserverport = flag.Int("udpserverport", 20000, "Port of the UDP server")
	logtofile     = false
)

func main() {
	wg := new(sync.WaitGroup)

	wg.Add(5)
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
	go intro.StartIntroducerAndListenToConnections(*devmode, *port, *udpserverport, wg)

	log.Printf("Starting the UDP server\n")
	go process.StartUdpServer(*udpserverport, wg)

	wg.Wait()
}
