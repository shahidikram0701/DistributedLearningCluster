package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os/exec"
	"strings"

	lg "cs425/mp1/logger_proto"

	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 50052, "The server port")
)

// server is used to implement helloworld.GreeterServer.
type server struct {
	lg.UnimplementedLoggerServer
}

// SayHello implements helloworld.GreeterServer
func (s *server) FindLogs(ctx context.Context, in *lg.FindLogsRequest) (*lg.FindLogsReply, error) {
	query := in.GetQuery()

	// TODO: error handling
	out, _ := (exec.Command("bash", "-c", "grep -H "+query+" ../logs/*.log").Output())
	res := string(out)
	numLines := strings.Count(res, "\n")

	return &lg.FindLogsReply{Logs: res, Count: int32(numLines)}, nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	lg.RegisterLoggerServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
