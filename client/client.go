package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
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

func main() {
	flag.Parse()
	var conn *grpc.ClientConn
	var err error
	var coordinatorIp string
	start := time.Now()
	for {
		if *devmode {
			coordinatorIp = "localhost:50051"
		} else {
			coordinatorIndex := rand.Intn(10)
			coordinatorIp = coordinators[coordinatorIndex]
		}

		log.Printf("Coordinator: %v", coordinatorIp)
		conn, err = grpc.Dial(coordinatorIp, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("Failed to establish connection with the coordinator....Retrying")
		}

		defer conn.Close()
		c := pb.NewCoordinatorClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		r, err := c.QueryLogs(ctx, &pb.QueryRequest{Query: *query, IsTest: false})

		if err != nil {
			log.Printf("Failed to establish connection with the coordinator....Retrying")
		} else {
			duration := time.Since(start)
			log.Printf("Successfully fetched logs")
			fmt.Printf(r.GetLogs())
			log.Printf("Total Matches: %v", r.GetTotalMatches())
			log.Printf("\nExecution duration: %v", duration)
			break
		}
	}
}
