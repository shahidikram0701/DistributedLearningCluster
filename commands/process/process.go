package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"

	config "cs425/mp/config"
	process "cs425/mp/process"
)

var (
	devmode = flag.Bool("devmode", false, "Develop locally?")
)

/**
* Get process's outbound address to add to membership list
 */
func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Printf("Couldn't get the IP address of the process\n%v", err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

func main() {
	flag.Parse()

	var env string
	if *devmode {
		env = "dev"
	} else {
		env = "prod"
	}
	configuration := config.GetConfig("../../config/config.json", env)
	outboundIp := GetOutboundIP()

	wg := new(sync.WaitGroup)
	wg.Add(4)
	if configuration.LogToFile {
		if _, err := os.Stat("../../logs"); os.IsNotExist(err) {
			err := os.Mkdir("../../logs", os.ModePerm)
			if err != nil {
				log.Panicf("Error creating logs folder\n")
			}
		}
		// write logs of the service process to process.log file
		f, err := os.OpenFile(fmt.Sprintf("../../logs/process-%v-%v.log", outboundIp, configuration.FailureDetectorPort), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("error opening file: %v", err)
		}
		defer f.Close()
		log.SetOutput(f)
	}

	introAddr := fmt.Sprintf("%s:%d", configuration.IntroducerAddress, configuration.IntroducerPort)

	// Start the process
	process.Run(configuration.FailureDetectorPort, configuration.UdpServerPort, configuration.LoggerPort, configuration.CoordinatorServiceLoggerPort, configuration.CoordinatorServiceSDFSPort, configuration.DataNodeServiceSDFSPort, wg, introAddr, *devmode, outboundIp)

	for {
		fmt.Printf("\n\nEnter command \n\t - printmembershiplist (To print memebership list)\n\t - printtopology\n\t - leave (To leave the network)\n\t - `${query-string}` (Enter a query string to search in the logs)\n\t - getallcoordinators (Get List of coordinators)\n\t - exit (To exit)\n\n\tSDFS commands\n\n\t - put (create or update a file)\n\t - get (get a file)\n\t - ls (List all nodes storing the file)\n\t - store (List all files stored in a node)\n\n\t: ")
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
		case "getallcoordinators":
			fmt.Printf("%v\n", process.GetAllCoordinators())
		case "exit":
			os.Exit(3)

		case "put":
			var filename string
			fmt.Printf("\tFilename: ")

			// Taking input from user
			fmt.Scanln(&filename)
			fmt.Println(process.PutFile(filename))

		case "ls":
			var filename string
			fmt.Printf("\tFilename: ")
			fmt.Scanln(&filename)
			fmt.Println(process.ListAllNodesForAFile(filename))

		case "store":
			fmt.Println(process.DataNode_ListAllFilesOnTheNode())

		case "get":
			var filename string
			fmt.Printf("\tFilename: ")

			// Taking input from user
			fmt.Scanln(&filename)
			fmt.Println(process.GetFile(filename))
		default:
			process.SendLogQueryRequest(configuration.CoordinatorServiceLoggerPort, command)
		}

	}

	// Wait for the wait group to be done
	// wg.Wait()
}
