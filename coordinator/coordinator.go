package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	pb "cs425/mp1/proto/coordinator_proto"
	lg "cs425/mp1/proto/logger_proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	devmode = flag.Bool("devmode", true, "Develop locally?")
	port    = flag.Int("port", 50051, "The server port")
)

var serverAddresses []string

type server struct {
	pb.UnimplementedCoordinatorServer
}

func addServerAddress(addr string) {
	serverAddresses = append(serverAddresses, addr)
}

func queryServer(addr string, query string, responseChannel chan *lg.FindLogsReply, sleeptime int) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := lg.NewLoggerClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r, err := c.FindLogs(ctx, &lg.FindLogsRequest{Query: query})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	// time.Sleep(time.Duration(sleeptime) * time.Second)
	responseChannel <- r
	// log.Printf("Greeting: %s", r.GetMessage())
}

func (s *server) QueryLogs(ctx context.Context, in *pb.QueryRequest) (*pb.QueryReply, error) {
	// log.Printf("Received: %v", in.GetQuery())

	// Establish connections with the server nodes
	responseChannel := make(chan *lg.FindLogsReply)

	for idx, addr := range serverAddresses {
		go queryServer(addr, in.GetQuery(), responseChannel, idx)
	}
	logs := ""
	matches:= 0
	for i := 0; i < len(serverAddresses); i++ {
		logQueryResponse := <-responseChannel
		logs += logQueryResponse.GetLogs()
		totalMatches += logQueryResponse.GetMatches()
	}
	return &pb.QueryReply{Logs: logs, Matches: totalMatches}, nil
}

func generateLogsOnServer(addr string, responseChannel chan *lg.GenerateLogsReply, filenumber int) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := lg.NewLoggerClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r, err := c.Test_GenerateLogs(ctx, &lg.GenerateLogsRequest{Filenumber: int32(filenumber)})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	// time.Sleep(time.Duration(sleeptime) * time.Second)
	responseChannel <- r
	// log.Printf("Greeting: %s", r.GetMessage())
}

func (s *server) Test_GenerateLogs(ctx context.Context, in *pb.GenerateLogsRequest) (*pb.GenerateLogsReply, error) {
	// Establish connections with the server nodes
	responseChannel := make(chan *lg.GenerateLogsReply)

	for idx, addr := range serverAddresses {
		go generateLogsOnServer(addr, responseChannel, idx+1)
	}
	status := ""
	for _, addr := range serverAddresses {
		generateLogsResponse := <-responseChannel
		status += addr + ":" + generateLogsResponse.GetStatus()
	}
	return &pb.GenerateLogsReply{Status: status}, nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterCoordinatorServer(s, &server{})

	if *devmode {
		// local testing

		addServerAddress("localhost:50052")
		// addServerAddress("localhost:50053")
	} else {
		// adding all the node endpoints to query

		addServerAddress("172.22.156.122:50052")
		addServerAddress("172.22.158.122:50052")
		addServerAddress("172.22.94.122:50052")
		addServerAddress("172.22.156.123:50052")
		addServerAddress("172.22.158.123:50052")
		addServerAddress("172.22.94.123:50052")
		addServerAddress("172.22.156.124:50052")
		addServerAddress("172.22.158.124:50052")
		addServerAddress("172.22.94.124:50052")
		addServerAddress("172.22.156.125:50052")
	}

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
