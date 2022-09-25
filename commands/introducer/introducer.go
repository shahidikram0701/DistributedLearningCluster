package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	intro "cs425/mp/introducer"
)

var (
	port          = flag.Int("port", 50053, "The port where the introducer runs")
	devmode       = flag.Bool("devmode", false, "Develop locally?")
	udpserverport = flag.Int("udpserverport", 20000, "Port of the UDP server")
	logtofile     = true
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

	// Start the introducer(has the TCP server) and UDP server processes
	intro.Run(*devmode, *port, *udpserverport, wg)

	for {
		fmt.Printf("\n\nEnter command \n\t - printmembershiplist (To print memebership list)\n\t - printtopology\n\t - exit (To exit)\n\n\t: ")
		var command string

		// Taking input from user
		fmt.Scanln(&command)

		switch command {
		case "printmembershiplist":
			fmt.Println(intro.GetMemberList().GetList())
		case "printtopology":
			fmt.Println(intro.GetNetworkTopology())
		case "exit":
			os.Exit(3)
		}

	}
}
