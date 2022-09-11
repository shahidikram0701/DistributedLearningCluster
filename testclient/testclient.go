package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	pb "cs425/mp1/proto/coordinator_proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultQuery = "shahid"
)

var (
	devmode      = flag.Bool("devmode", false, "Develop locally?")
	query        = flag.String("query", defaultQuery, "Query to search for")
	coordinators = []string{"172.22.156.122:50051", "172.22.158.122:50051", "172.22.94.122:50051", "172.22.156.123:50051", "172.22.158.123:50051", "172.22.94.123:50051", "172.22.156.124:50051", "172.22.158.124:50051", "172.22.94.124:50051", "172.22.156.125:50051"}
)

func Test1() {
	log.Printf("\n\nTEST 1: Returns matches for normal query\n")
	log.Printf("\ngrep -Ec 'privacy'\n")

	flag.Parse()
	var conn *grpc.ClientConn
	var err error
	var coordinatorIp string
	for {
		if *devmode {
			coordinatorIp = "localhost:50051"
		} else {
			coordinatorIndex := rand.Intn(10)
			coordinatorIp = coordinators[coordinatorIndex]
		}

		log.Printf("Coordinator: %v\n", coordinatorIp)
		conn, err = grpc.Dial(coordinatorIp, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to establish connection with the coordinator....Retrying")
		}

		defer conn.Close()
		c := pb.NewCoordinatorClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		start := time.Now()
		r, err := c.QueryLogs(ctx, &pb.QueryRequest{Query: "privacy", IsTest: true})

		if err != nil {
			log.Printf("Failed to establish connection with the coordinator....Retrying")
		} else {
			duration := time.Since(start)
			ValidateTest1(r)
			log.Printf("\nExecution duration: %v", duration)
			break
		}
	}
}

func ValidateTest1(r *pb.QueryReply) {
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
			if numMatches != "16199" {
				fmt.Println("[FAIL] vm1.log should have 16199 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm2.log":
			if numMatches != "15448" {
				fmt.Println("[FAIL] vm2.log should have 15448 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm3.log":
			if numMatches != "15329" {
				fmt.Println("[FAIL] vm3.log should have 15329 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm4.log":
			if numMatches != "15431" {
				fmt.Println("[FAIL] vm4.log should have 15431 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm5.log":
			if numMatches != "15433" {
				fmt.Println("[FAIL] vm5.log should have 15433 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm6.log":
			if numMatches != "15350" {
				fmt.Println("[FAIL] vm6.log should have 15350 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm7.log":
			if numMatches != "15216" {
				fmt.Println("[FAIL] vm7.log should have 15216 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm8.log":
			if numMatches != "15485" {
				fmt.Println("[FAIL] vm8.log should have 15485 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm9.log":
			if numMatches != "15369" {
				fmt.Println("[FAIL] vm9.log should have 15369 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm10.log":
			if numMatches != "15127" {
				fmt.Println("[FAIL] vm10.log should have 15127 matches for the given query")
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

func Test2() {
	log.Printf("\n\nTEST 2: should return matches for infrequent type query\n")
	log.Printf("\ngrep -Ec 'http://www.burke.com/homepage.html'\n")

	flag.Parse()
	var conn *grpc.ClientConn
	var err error
	var coordinatorIp string
	for {
		if *devmode {
			coordinatorIp = "localhost:50051"
		} else {
			coordinatorIndex := rand.Intn(10)
			coordinatorIp = coordinators[coordinatorIndex]
		}

		log.Printf("Coordinator: %v\n", coordinatorIp)
		conn, err = grpc.Dial(coordinatorIp, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to establish connection with the coordinator....Retrying")
		}

		defer conn.Close()
		c := pb.NewCoordinatorClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		start := time.Now()
		r, err := c.QueryLogs(ctx, &pb.QueryRequest{Query: "http://www.burke.com/homepage.html", IsTest: true})

		if err != nil {
			log.Printf("Failed to establish connection with the coordinator....Retrying")
		} else {
			duration := time.Since(start)
			ValidateTest2(r)
			log.Printf("\nExecution duration: %v", duration)
			break
		}
	}
}

func ValidateTest2(r *pb.QueryReply) {
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
			if numMatches != "1" {
				fmt.Println("[FAIL] vm1.log should have 1 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm2.log":
			if numMatches != "1" {
				fmt.Println("[FAIL] vm2.log should have 1 matches for the given query")
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
			if numMatches != "1" {
				fmt.Println("[FAIL] vm7.log should have 1 matches for the given query")
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

func Test3() {
	log.Printf("\n\nTEST 3: Regex query\n")
	log.Printf("\ngrep -Ec 'http:/*'\n")

	flag.Parse()
	var conn *grpc.ClientConn
	var err error
	var coordinatorIp string
	for {
		if *devmode {
			coordinatorIp = "localhost:50051"
		} else {
			coordinatorIndex := rand.Intn(10)
			coordinatorIp = coordinators[coordinatorIndex]
		}

		log.Printf("Coordinator: %v\n", coordinatorIp)
		conn, err = grpc.Dial(coordinatorIp, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to establish connection with the coordinator....Retrying")
		}

		defer conn.Close()
		c := pb.NewCoordinatorClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		start := time.Now()
		r, err := c.QueryLogs(ctx, &pb.QueryRequest{Query: "http:/*", IsTest: true})

		if err != nil {
			log.Printf("Failed to establish connection with the coordinator....Retrying")
		} else {
			duration := time.Since(start)
			ValidateTest3(r)
			log.Printf("\nExecution duration: %v", duration)
			break
		}
	}
}

func ValidateTest3(r *pb.QueryReply) {
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
			if numMatches != "283553" {
				fmt.Println("[FAIL] vm1.log should have 283553 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm2.log":
			if numMatches != "267938" {
				fmt.Println("[FAIL] vm2.log should have 267938 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm3.log":
			if numMatches != "268804" {
				fmt.Println("[FAIL] vm3.log should have 268804 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm4.log":
			if numMatches != "270917" {
				fmt.Println("[FAIL] vm4.log should have 270917 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm5.log":
			if numMatches != "271205" {
				fmt.Println("[FAIL] vm5.log should have 271205 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm6.log":
			if numMatches != "268894" {
				fmt.Println("[FAIL] vm6.log should have 268894 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm7.log":
			if numMatches != "268084" {
				fmt.Println("[FAIL] vm7.log should have 268084 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm8.log":
			if numMatches != "274522" {
				fmt.Println("[FAIL] vm8.log should have 274522 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm9.log":
			if numMatches != "269822" {
				fmt.Println("[FAIL] vm9.log should have 269822 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm10.log":
			if numMatches != "265524" {
				fmt.Println("[FAIL] vm10.log should have 265524 matches for the given query")
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

func Test4() {
	log.Printf("\n\nTEST 4: Fetch all the logs in the month of August\n")
	log.Printf("\ngrep -Ec '\\[(0?[1-9]|[12][0-9]|3[01])/Aug/([0-9]+(:[0-9]+)+) -[0-9]+]'\n")

	flag.Parse()
	var conn *grpc.ClientConn
	var err error
	var coordinatorIp string
	for {
		if *devmode {
			coordinatorIp = "localhost:50051"
		} else {
			coordinatorIndex := rand.Intn(10)
			coordinatorIp = coordinators[coordinatorIndex]
		}

		log.Printf("Coordinator: %v\n", coordinatorIp)
		conn, err = grpc.Dial(coordinatorIp, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to establish connection with the coordinator....Retrying")
		}

		defer conn.Close()
		c := pb.NewCoordinatorClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		start := time.Now()
		r, err := c.QueryLogs(ctx, &pb.QueryRequest{Query: "\\[(0?[1-9]|[12][0-9]|3[01])/Aug/([0-9]+(:[0-9]+)+) -[0-9]+]", IsTest: true})

		if err != nil {
			log.Printf("Failed to establish connection with the coordinator....Retrying")
		} else {
			duration := time.Since(start)
			ValidateTest4(r)
			log.Printf("\nExecution duration: %v", duration)
			break
		}
	}
}

func ValidateTest4(r *pb.QueryReply) {
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
			if numMatches != "23644" {
				fmt.Println("[FAIL] vm1.log should have 23644 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm2.log":
			if numMatches != "23733" {
				fmt.Println("[FAIL] vm2.log should have 23733 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm3.log":
			if numMatches != "23578" {
				fmt.Println("[FAIL] vm3.log should have 23578 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm4.log":
			if numMatches != "23717" {
				fmt.Println("[FAIL] vm4.log should have 23717 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm5.log":
			if numMatches != "23743" {
				fmt.Println("[FAIL] vm5.log should have 23743 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm6.log":
			if numMatches != "23595" {
				fmt.Println("[FAIL] vm6.log should have 23595 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm7.log":
			if numMatches != "23615" {
				fmt.Println("[FAIL] vm7.log should have 23615 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm8.log":
			if numMatches != "23792" {
				fmt.Println("[FAIL] vm8.log should have 23792 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm9.log":
			if numMatches != "23691" {
				fmt.Println("[FAIL] vm9.log should have 23691 matches for the given query")
			} else {
				fmt.Printf("[PASS] Number of matches in %v is correct\n", filename)
			}
		case "vm10.log":
			if numMatches != "23560" {
				fmt.Println("[FAIL] vm10.log should have 23560 matches for the given query")
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

func Test5() {
	log.Printf("\n\nTEST 5: Query doesn't exist\n")
	log.Printf("\ngrep -Ec 'this query doesnt exist'\n")

	flag.Parse()
	var conn *grpc.ClientConn
	var err error
	var coordinatorIp string
	for {
		if *devmode {
			coordinatorIp = "localhost:50051"
		} else {
			coordinatorIndex := rand.Intn(10)
			coordinatorIp = coordinators[coordinatorIndex]
		}

		log.Printf("Coordinator: %v\n", coordinatorIp)
		conn, err = grpc.Dial(coordinatorIp, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to establish connection with the coordinator....Retrying")
		}

		defer conn.Close()
		c := pb.NewCoordinatorClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		start := time.Now()
		r, err := c.QueryLogs(ctx, &pb.QueryRequest{Query: "this query doesnt exist", IsTest: true})

		if err != nil {
			log.Printf("Failed to establish connection with the coordinator....Retrying")
		} else {
			duration := time.Since(start)
			ValidateTest5(r)
			log.Printf("\nExecution duration: %v", duration)
			break
		}
	}
}

func ValidateTest5(r *pb.QueryReply) {
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

	Test1()

	Test2()

	Test3()

	Test4()

	Test5()
}
