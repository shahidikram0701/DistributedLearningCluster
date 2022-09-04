package main

import (
	"context"
	"flag"
	"log"
	"time"

	pb "cs425/mp1/coordinator_proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultCommand = "shahid"
)

var (
	addr  = flag.String("addr", "localhost:50051", "the address to connect to")
	query = flag.String("query", defaultCommand, "Query to search for")
)

func main() {
	flag.Parse()
	// Set up a connection to the server.
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewCoordinatorClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := c.QueryLogs(ctx, &pb.QueryRequest{Query: *query})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("Number of Matches: %v", r.Matches)
	log.Printf(r.GetLogs())
}
