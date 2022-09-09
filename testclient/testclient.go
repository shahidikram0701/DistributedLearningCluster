package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	pb "cs425/mp1/proto/coordinator_proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultQuery         = "shahid"
	defaultCoordinatorIp = "172.22.156.122:50051"
)

var (
	coordinatorIp = flag.String("coordinatorip", defaultCoordinatorIp, "Coordinator IP")
	query         = flag.String("query", defaultQuery, "Query to search for")
)

func Test1(c pb.CoordinatorClient, ctx context.Context) {
	log.Printf("TEST 1")
	log.Printf("\n\ngrep -Ec 'Azure'\n\n")
	r, err := c.QueryLogs(ctx, &pb.QueryRequest{Query: "Azure", IsTest: true})
	if err != nil {
		log.Fatalf("Failed to query logs: %v", err)
	}
	logs := r.GetLogs()
	fmt.Println(logs)
	logList := strings.Split(logs, "\n")

	for _, log := range logList[:len(logList)-1] {
		fileAndNumMatches := strings.Split(log, ":")
		filenameSplit := strings.Split(fileAndNumMatches[0], "/")
		filename := filenameSplit[len(filenameSplit)-1]
		numMatches := fileAndNumMatches[1]

		switch filename {
		case "vm1.log":
			if numMatches != "1000" {
				fmt.Println("[FAIL] vm1.log should have 1000 matches for the given query")
			} else {
				fmt.Println("[PASS]")
			}
		case "vm2.log":
			if numMatches != "900" {
				fmt.Println("[FAIL] vm2.log should have 1000 matches for the given query")
			} else {
				fmt.Println("[PASS]")
			}
		case "vm3.log":
			if numMatches != "2300" {
				fmt.Println("[FAIL] vm3.log should have 1000 matches for the given query")
			} else {
				fmt.Println("[PASS]")
			}
		case "vm4.log":
			if numMatches != "0" {
				fmt.Println("[FAIL] vm4.log should have 1000 matches for the given query")
			} else {
				fmt.Println("[PASS]")
			}
		case "vm5.log":
			if numMatches != "2424" {
				fmt.Println("[FAIL] vm5.log should have 1000 matches for the given query")
			} else {
				fmt.Println("[PASS]")
			}
		case "vm6.log":
			if numMatches != "0" {
				fmt.Println("[FAIL] vm6.log should have 1000 matches for the given query")
			} else {
				fmt.Println("[PASS]")
			}
		case "vm7.log":
			if numMatches != "3064" {
				fmt.Println("[FAIL] vm1.log should have 1000 matches for the given query")
			} else {
				fmt.Println("[PASS]")
			}
		case "vm8.log":
			if numMatches != "2666" {
				fmt.Println("[FAIL] vm8.log should have 1000 matches for the given query")
			} else {
				fmt.Println("[PASS]")
			}
		case "vm9.log":
			if numMatches != "1201" {
				fmt.Println("[FAIL] vm9.log should have 1000 matches for the given query")
			} else {
				fmt.Println("[PASS]")
			}
		case "vm10.log":
			if numMatches != "2910" {
				fmt.Println("[FAIL] vm1.log should have 1000 matches for the given query")
			} else {
				fmt.Println("[PASS]")
			}
		}

	}
}

func main() {
	flag.Parse()
	// Set up a connection to the server.
	conn, err := grpc.Dial(*coordinatorIp, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to establish connection with the coordinator")
	}
	defer conn.Close()
	c := pb.NewCoordinatorClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := c.Test_GenerateLogs(ctx, &pb.GenerateLogsRequest{})
	if err != nil {
		log.Fatalf("Failed to generate Logs: %v", err)
	}
	log.Println(r.GetStatus())

	Test1(c, ctx)
}
