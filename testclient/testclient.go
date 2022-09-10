package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strconv"
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
	log.Printf("TEST 1: Returns matches for normal query")
	log.Printf("\n\ngrep -Ec 'Azure'\n\n")
	r, err := c.QueryLogs(ctx, &pb.QueryRequest{Query: "Azure", IsTest: true})
	if err != nil {
		log.Fatalf("Failed to query logs: %v", err)
	}
	logs := r.GetLogs()
	fmt.Println(logs)
	logList := strings.Split(logs, "\n")
	totalMatches := 0

	for _, log := range logList[:len(logList)-1] {
		fileAndNumMatches := strings.Split(log, ":")
		filenameSplit := strings.Split(fileAndNumMatches[0], "/")
		filename := filenameSplit[len(filenameSplit)-1]
		numMatches := fileAndNumMatches[1]
		numMatchesInt, _ := strconv.Atoi(numMatches)
		totalMatches += numMatchesInt

		switch filename {
		case "vm1.log":
			if numMatches != "1000" {
				fmt.Println("[FAIL] vm1.log should have 1000 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm2.log":
			if numMatches != "900" {
				fmt.Println("[FAIL] vm2.log should have 900 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm3.log":
			if numMatches != "2300" {
				fmt.Println("[FAIL] vm3.log should have 2300 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm4.log":
			if numMatches != "0" {
				fmt.Println("[FAIL] vm4.log should have 0 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm5.log":
			if numMatches != "2424" {
				fmt.Println("[FAIL] vm5.log should have 2424 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm6.log":
			if numMatches != "0" {
				fmt.Println("[FAIL] vm6.log should have 0 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm7.log":
			if numMatches != "3064" {
				fmt.Println("[FAIL] vm7.log should have 3064 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm8.log":
			if numMatches != "2666" {
				fmt.Println("[FAIL] vm8.log should have 2666 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm9.log":
			if numMatches != "1201" {
				fmt.Println("[FAIL] vm9.log should have 1201 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm10.log":
			if numMatches != "2910" {
				fmt.Println("[FAIL] vm10.log should have 2910 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		}
	}
	if totalMatches == int(r.GetTotalMatches()) {
		fmt.Printf("[PASS] Total count of matches is correct\n")
	} else {
		fmt.Printf("[FAIL] Total Number of matches should be %v but is %v\n", r.GetTotalMatches(), int(totalMatches))
	}
}

func Test2(c pb.CoordinatorClient, ctx context.Context) {
	log.Printf("TEST 2: should return matches for regex type query")
	log.Printf("\n\ngrep -Ec 't*'\n\n")
	r, err := c.QueryLogs(ctx, &pb.QueryRequest{Query: "t*", IsTest: true})
	if err != nil {
		log.Fatalf("Failed to query logs: %v", err)
	}
	logs := r.GetLogs()
	fmt.Println(logs)
	logList := strings.Split(logs, "\n")
	totalMatches := 0

	for _, log := range logList[:len(logList)-1] {
		fileAndNumMatches := strings.Split(log, ":")
		filenameSplit := strings.Split(fileAndNumMatches[0], "/")
		filename := filenameSplit[len(filenameSplit)-1]
		numMatches := fileAndNumMatches[1]
		numMatchesInt, _ := strconv.Atoi(numMatches)
		totalMatches += numMatchesInt

		switch filename {
		case "vm1.log":
			if numMatches != "2000" {
				fmt.Println("[FAIL] vm1.log should have 2000 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm2.log":
			if numMatches != "1800" {
				fmt.Println("[FAIL] vm2.log should have 1800 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm3.log":
			if numMatches != "4600" {
				fmt.Println("[FAIL] vm3.log should have 4600 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm4.log":
			if numMatches != "4260" {
				fmt.Println("[FAIL] vm4.log should have 4260 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm5.log":
			if numMatches != "2424" {
				fmt.Println("[FAIL] vm5.log should have 2424 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm6.log":
			if numMatches != "1818" {
				fmt.Println("[FAIL] vm6.log should have 1818 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm7.log":
			if numMatches != "3064" {
				fmt.Println("[FAIL] vm7.log should have 3064 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm8.log":
			if numMatches != "2666" {
				fmt.Println("[FAIL] vm8.log should have 2666 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm9.log":
			if numMatches != "2402" {
				fmt.Println("[FAIL] vm9.log should have 2402 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm10.log":
			if numMatches != "5820" {
				fmt.Println("[FAIL] vm10.log should have 5820 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		}
	}
	if totalMatches == int(r.GetTotalMatches()) {
		fmt.Printf("[PASS] Total count of matches is correct\n")
	} else {
		fmt.Printf("[FAIL] Total Number of matches should be %v but is %v\n", r.GetTotalMatches(), int(totalMatches))
	}
}

func Test3(c pb.CoordinatorClient, ctx context.Context) {
	log.Printf("TEST 3: Query doesn't exist")
	log.Printf("\n\ngrep -Ec 'generation'\n\n")
	r, err := c.QueryLogs(ctx, &pb.QueryRequest{Query: "generation", IsTest: true})
	if err != nil {
		log.Fatalf("Failed to query logs: %v", err)
	}
	logs := r.GetLogs()
	fmt.Println(logs)
	logList := strings.Split(logs, "\n")
	totalMatches := 0

	for _, log := range logList[:len(logList)-1] {
		fileAndNumMatches := strings.Split(log, ":")
		filenameSplit := strings.Split(fileAndNumMatches[0], "/")
		filename := filenameSplit[len(filenameSplit)-1]
		numMatches := fileAndNumMatches[1]
		numMatchesInt, _ := strconv.Atoi(numMatches)
		totalMatches += numMatchesInt

		switch filename {
		case "vm1.log":
			if numMatches != "0" {
				fmt.Println("[FAIL] vm1.log should have 0 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm2.log":
			if numMatches != "0" {
				fmt.Println("[FAIL] vm2.log should have 0 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm3.log":
			if numMatches != "0" {
				fmt.Println("[FAIL] vm3.log should have 0 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm4.log":
			if numMatches != "0" {
				fmt.Println("[FAIL] vm4.log should have 0 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm5.log":
			if numMatches != "0" {
				fmt.Println("[FAIL] vm5.log should have 0 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm6.log":
			if numMatches != "0" {
				fmt.Println("[FAIL] vm6.log should have 0 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm7.log":
			if numMatches != "0" {
				fmt.Println("[FAIL] vm7.log should have 0 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm8.log":
			if numMatches != "0" {
				fmt.Println("[FAIL] vm8.log should have 0 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm9.log":
			if numMatches != "0" {
				fmt.Println("[FAIL] vm9.log should have 0 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm10.log":
			if numMatches != "0" {
				fmt.Println("[FAIL] vm10.log should have 0 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		}
	}
	if totalMatches == int(r.GetTotalMatches()) {
		fmt.Printf("[PASS] Total count of matches is correct\n")
	} else {
		fmt.Printf("[FAIL] Total Number of matches should be %v but is %v\n", r.GetTotalMatches(), int(totalMatches))
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

	Test2(c, ctx)

	Test3(c, ctx)
}
