package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	pb "cs425/mp1/coordinator_proto"
	lg "cs425/mp1/logger_proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	port = flag.Int("port", 50051, "The server port")
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
	for i := 0; i < len(serverAddresses); i++ {
		logQueryResponse := <-responseChannel
		logs += logQueryResponse.GetLogs()
	}
	return &pb.QueryReply{Logs: logs}, nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterCoordinatorServer(s, &server{})

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

	// local testing
	// addServerAddress("localhost:50052")
	// addServerAddress("192.168.0.102:50053")
	// addServerAddress("192.168.0.102:50054")

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
