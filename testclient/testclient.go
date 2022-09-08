package main

import (
	"context"
	"flag"
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
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewCoordinatorClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r, err := c.Test_GenerateLogs(ctx, &pb.GenerateLogsRequest{})
	if err != nil {
		log.Fatalf("could not Generate Logs: %v", err)
	}
	log.Println(r.GetStatus())
}
