package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"

	lg "cs425/mp1/proto/logger_proto"

	"google.golang.org/grpc"
)

var (
	port = flag.Int("port", 50052, "The server port")
)

type server struct {
	lg.UnimplementedLoggerServer
}

/**
* The RPC function for processing the request to fetch logs
*
* @param ctx: context
* @param in: the query request
 */
func (s *server) FindLogs(ctx context.Context, in *lg.FindLogsRequest) (*lg.FindLogsReply, error) {
	query := in.GetQuery()
	isTest := in.GetIsTest()
	tag := ""
	if isTest {
		tag = "[ TEST ]"
	}

	var logFilePath string
	// if isTest {
	// 	logFilePath = "../../testlogs/*.log"
	// } else {
	// 	logFilePath = "../../logs/*.log"
	// }
	logFilePath = "../../logs/*.log"
	grepCommand := fmt.Sprintf("grep -HEc '%v' %v", query, logFilePath)

	log.Printf("%vExecuting: %v", tag, grepCommand)

	// Exectute the underlying os grep command for the given
	out, _ := (exec.Command("bash", "-c", grepCommand).Output())
	res := string(out)

	logData := strings.Split(strings.Split(res, "\n")[0], ":")
	numMatches, _ := strconv.Atoi(logData[len(logData)-1])

	return &lg.FindLogsReply{Logs: res, NumMatches: int64(numMatches)}, nil
}

/**
* The RPC function to process the generation of logs of service processes
*
* @param ctx: context
* @param in: the query request
 */
func (s *server) Test_GenerateLogs(ctx context.Context, in *lg.GenerateLogsRequest) (*lg.GenerateLogsReply, error) {
	// use the filenumber passed in the request to determine which script to execute to generate logs
	// for the current process
	filenumber := fmt.Sprint(in.GetFilenumber())
	outputFile := fmt.Sprintf("../../testlogs/vm%v.log", filenumber)
	command := "../test_log_scripts/log_gen" + filenumber + ".sh > " + outputFile
	var status = "Successfully generated logs"

	if _, err := os.Stat(outputFile); err != nil {
		log.Printf("command: %v", command)

		_, err := (exec.Command("bash", "-c", command).Output())

		if err != nil {
			status = "Failed to generate logs"
			log.Printf("Failed to generate logs")
		}
	}

	return &lg.GenerateLogsReply{Status: status}, nil
}

func main() {
	flag.Parse()
	// write logs of the service process to service.log file
	f, err := os.OpenFile("service.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Printf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)

	// service process listening to incoming tcp connections
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
