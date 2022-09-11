package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	pb "cs425/mp1/proto/coordinator_proto"
	lg "cs425/mp1/proto/logger_proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	devmode = flag.Bool("devmode", false, "Develop locally?")
	port    = flag.Int("port", 50051, "The server port")
)

var serverAddresses []string

type server struct {
	pb.UnimplementedCoordinatorServer
}

func addServerAddress(addr string) {
	serverAddresses = append(serverAddresses, addr)
}

func queryServer(addr string, query string, isTest bool, responseChannel chan *lg.FindLogsReply) {
	tag := ""
	if isTest {
		tag = "[ TEST ]"
	}
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("%vCould not connect to node: %v", tag, addr)
	}
	defer conn.Close()
	c := lg.NewLoggerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	r, err := c.FindLogs(ctx, &lg.FindLogsRequest{Query: query, IsTest: isTest})
	if err != nil {
		log.Printf("%vCould not connect to node: %v", tag, addr)
	}
	responseChannel <- r
}

func (s *server) QueryLogs(ctx context.Context, in *pb.QueryRequest) (*pb.QueryReply, error) {
	// Establish connections with the server nodes
	responseChannel := make(chan *lg.FindLogsReply)

	for _, addr := range serverAddresses {
		go queryServer(addr, in.GetQuery(), in.GetIsTest(), responseChannel)
	}
	logs := ""
	totalMatches := 0
	for i := 0; i < len(serverAddresses); i++ {
		logQueryResponse := <-responseChannel
		logs += logQueryResponse.GetLogs()
		totalMatches += int(logQueryResponse.GetNumMatches())
	}
	return &pb.QueryReply{Logs: logs, TotalMatches: int64(totalMatches)}, nil
}

func generateLogsOnServer(addr string, responseChannel chan *lg.GenerateLogsReply, filenumber int) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Could not connect to node: %v", addr)
	}
	defer conn.Close()
	c := lg.NewLoggerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := c.Test_GenerateLogs(ctx, &lg.GenerateLogsRequest{Filenumber: int32(filenumber)})
	if err != nil {
		log.Printf("Failed to generate Logs in: %v", addr)
	}
	responseChannel <- r
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
	f, err := os.OpenFile("coordinator.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Printf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterCoordinatorServer(s, &server{})

	if *devmode {
		// local testing
		addServerAddress("localhost:50052")
		addServerAddress("localhost:50053")
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
