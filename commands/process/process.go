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
	logtofile         = true
	introducerAddress = "172.22.156.122"
	introducerPort    = 50053
	udpserverport     = flag.Int("udpserverport", 20000, "Port of the UDP server")
)

func main() {
	port := flag.Int("port", 50054, "The failure detector process port")
	flag.Parse()
	log.Printf("port: %v", *port)
	wg := new(sync.WaitGroup)
	wg.Add(4)
	if logtofile {
		// write logs of the service process to process.log file
		f, err := os.OpenFile(fmt.Sprintf("process-%v.log", *port), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("error opening file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	if *devmode {
		introducerAddress = "localhost"
	}

	introAddr := fmt.Sprintf("%s:%d", introducerAddress, introducerPort)

	// Joins the network and starts the UDP server
	process.Run(*port, *udpserverport, *log_process_port, wg, introAddr)

	for {
		fmt.Printf("\n\nEnter command \n\t - printmembershiplist (To print memebership list)\n\t - printtopology\n\t - leave (To leave the network)\n\t - exit (To exit)\n\n\t: ")
		var command string

		// Taking input from user
		fmt.Scanln(&command)

		switch command {
		case "leave":
			process.LeaveNetwork()
		case "printmembershiplist":
			fmt.Println(process.GetMemberList().GetList())
		case "printtopology":
			fmt.Println(process.GetNetworkTopology())
		case "exit":
			os.Exit(3)
		}

	}

	// Wait for the wait group to be done
	// wg.Wait()
}
