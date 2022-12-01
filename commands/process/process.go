package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

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
	process.Run(configuration.FailureDetectorPort, configuration.UdpServerPort, configuration.LoggerPort, configuration.CoordinatorServiceLoggerPort, configuration.CoordinatorServiceSDFSPort, configuration.DataNodeServiceSDFSPort, wg, introAddr, *devmode, outboundIp, configuration.SchedulerPort, configuration.WorkerPort)

	for {
		fmt.Printf("\n\nEnter command \n\t - printmembershiplist (To print memebership list)\n\t - printtopology\n\t - leave (To leave the network)\n\t - search-logs <query> (Enter a query string to search in the logs)\n\t - getallcoordinators (Get List of coordinators)\n\t - exit (To exit)\n\n\tSDFS commands\n\n\t - put <localfilename> <sdfsfilename>\n\t - get <sdfsfilename> <localfilename>\n\t - delete <sdfsfilename>\n\t - ls <sdfsfilename>\n\t - store\n\t - get-versions <sdfsfilename> <numVersions> <localfilename>\n\n\tIDunno commands\n\n\t - deploy-model <modelname>\n\t - query-model <modelname> <queryinputfilename>\n\t - start-inference <modelname> <numinputfiles> <files...>\n\t - get-tasks\n\t - get-model-tasks <modelname>\n\n\t: ")

		inputReader := bufio.NewReader(os.Stdin)
		command, _ := inputReader.ReadString('\n')
		command = strings.TrimSuffix(command, "\n")
		parsedCommand := strings.Split(command, " ")

		switch parsedCommand[0] {
		case "leave":
			process.LeaveNetwork()
		case "printmembershiplist":
			fmt.Println("\n", process.GetMemberList().GetList())
		case "printtopology":
			fmt.Println("\n", process.GetNetworkTopology())
		case "getallcoordinators":
			fmt.Printf("\n%v\n", process.GetAllCoordinators())
		case "exit":
			os.Exit(3)

		case "put":
			if len(parsedCommand) <= 2 {
				fmt.Printf("\n\tSpecify both localfilename and filename")
				continue
			}
			localfilename := parsedCommand[1]
			filename := parsedCommand[2]
			start := time.Now()
			status := process.PutFile(filename, localfilename)
			fmt.Printf("Status: %v\tTime taken: %v\n\t", status, time.Since(start).Seconds())

		case "ls":
			if len(parsedCommand) <= 1 {
				fmt.Printf("\n\tSpecify filename")
				continue
			}
			filename := parsedCommand[1]
			fmt.Println("\n\t", process.ListAllNodesForAFile(filename))

		case "store":
			fmt.Println("\n\t", process.DataNode_ListAllFilesOnTheNode())

		case "get":
			if len(parsedCommand) <= 2 {
				fmt.Printf("\n\tSpecify filename")
				continue
			}
			filename := parsedCommand[1]
			localfilename := parsedCommand[2]
			start := time.Now()
			status := process.GetFile(filename, localfilename, false)
			fmt.Printf("Status: %v\tTime taken: %v\n\t", status, time.Since(start).Seconds())

		case "delete":
			if len(parsedCommand) <= 1 {
				fmt.Printf("\n\tSpecify filename")
				continue
			}
			filename := parsedCommand[1]
			start := time.Now()
			status := process.DeleteFile(filename)

			fmt.Printf("Status: %v\tTime taken: %v\n\t", status, time.Since(start).Seconds())

		case "get-versions":
			if len(parsedCommand) <= 3 {
				fmt.Printf("\n\tSpecify filename and versions")
				continue
			}
			filename := parsedCommand[1]
			numVersions, _ := strconv.Atoi(parsedCommand[2])
			localfilename := parsedCommand[3]
			start := time.Now()
			status := process.GetFileVersions(filename, numVersions, localfilename)
			fmt.Printf("Status: %v\tTime taken: %v\n\t", status, time.Since(start).Seconds())

		case "search-logs":
			if len(parsedCommand) <= 1 {
				fmt.Printf("\n\tSpecify query")
				continue
			}
			query := parsedCommand[1]
			process.SendLogQueryRequest(configuration.CoordinatorServiceLoggerPort, query)

		case "deploy-model":
			if len(parsedCommand) <= 1 {
				fmt.Printf("\n\tSpecify model name")
				continue
			}
			modelname := parsedCommand[1]
			start := time.Now()
			fmt.Printf("Status: %v\t Time Taken: %vs\n\t;", process.DeployModel(modelname), time.Since(start).Seconds())

		case "query-model":
			if len(parsedCommand) <= 2 {
				fmt.Printf("\n\tSpecify model name and query input file")
				continue
			}
			modelname := parsedCommand[1]
			queryinputfilename := parsedCommand[2]
			fmt.Printf("%v\n", process.QueryModel(modelname, []string{queryinputfilename}))

		case "start-inference":
			if len(parsedCommand) <= 3 {
				fmt.Printf("\n\nSpecify input filenames")
				continue
			}
			modelname := parsedCommand[1]
			numfiles, _ := strconv.Atoi(parsedCommand[2])
			files := []string{}

			for f := 0; f < numfiles; f++ {
				files = append(files, parsedCommand[3+f])
			}
			process.StartInference(modelname, files)

		case "get-tasks":
			tasks := process.GetAllTasks()
			fmt.Println("Tasks:")
			for _, task := range tasks {
				fmt.Println(task)
			}

		case "get-model-tasks":
			if len(parsedCommand) <= 1 {
				fmt.Printf("\n\tSpecify model name")
				continue
			}
			modelname := parsedCommand[1]
			tasks := process.GetAllTasksOfModel(modelname)
			fmt.Println("Tasks:")
			for _, task := range tasks {
				fmt.Println(task)
			}

		default:
			continue
		}

	}

	// Wait for the wait group to be done
	// wg.Wait()
}
