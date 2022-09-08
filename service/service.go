package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os/exec"
	"strings"

	lg "cs425/mp1/proto/logger_proto"

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
	// log.Printf("Received: %v", query)
	// TODO: error handling
	out, _ := (exec.Command("bash", "-c", "grep -HEc '"+query+"' ../../logs/*.log").Output())
	res := string(out)
	matches:= int32(strings.Split(res, ":")[1]);

	return &lg.FindLogsReply{Logs: res, Matches: matches}, nil
}

func (s *server) Test_GenerateLogs(ctx context.Context, in *lg.GenerateLogsRequest) (*lg.GenerateLogsReply, error) {
	filenumber := fmt.Sprint(in.GetFilenumber())
	log.Printf("Received: %v", filenumber)
	// TODO: error handling
	command := "../test_log_scripts/log_gen" + filenumber + ".sh > ../../testlogs/vm" + filenumber + ".log"
	log.Printf("command: %v", command)
	var status string
	_, err := (exec.Command("bash", "-c", command).Output())

	if err != nil {
		status = "Failed to generate logs"
	} else {
		status = "Successfully generated logs"
	}

	return &lg.GenerateLogsReply{Status: status}, nil
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
