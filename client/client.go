package main

import (
	"context"
	"flag"
	"fmt"
	"log"
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r, err := c.QueryLogs(ctx, &pb.QueryRequest{Query: *query, IsTest: false})
	if err != nil {
		log.Fatalf("Failed to query logs: %v", err)
	}
	log.Printf("Successfully fetched logs")
	fmt.Printf(r.GetLogs())
	log.Printf("Total Matches: %v", r.GetTotalMatches())
}
