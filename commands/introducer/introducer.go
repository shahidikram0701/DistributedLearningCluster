package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"cs425/mp/config"
	intro "cs425/mp/introducer"
)

var (
	devmode = flag.Bool("devmode", false, "Develop locally?")
)

func main() {
	wg := new(sync.WaitGroup)

	wg.Add(5)
	flag.Parse()

	var env string
	if *devmode {
		env = "dev"
	} else {
		env = "prod"
	}
	configuration := config.GetConfig("../../config/config.json", env)

	if configuration.LogToFile {
		// write logs of the service process to introducer.log file
		if _, err := os.Stat("../../logs"); os.IsNotExist(err) {
			err := os.Mkdir("../../logs", os.ModePerm)
			if err != nil {
				log.Panicf("Error creating logs folder\n")
			}
		}
		f, err := os.OpenFile("../../logs/introducer.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("error opening file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	// Start the introducer
	intro.Run(*devmode, configuration.IntroducerPort, configuration.UdpServerPort, wg)

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
