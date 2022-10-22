package process

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"cs425/mp/config"
	pb "cs425/mp/proto/coordinator_proto"
	lg "cs425/mp/proto/logger_proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// list of machines acting as workers
var serverAddresses []string

type CoordinatorServer struct {
	pb.UnimplementedCoordinatorServer
}

/**
* Adds an address to the list of all machine addresses
*
* @param addr: IP address of the machine
 */
func addServerAddress(addr string) {
	serverAddresses = append(serverAddresses, addr)
}

/**
* Send the query to the service process
*
* @param addr: IP address of the worker/service process
* @param query: query string
* @param isTest: boolean indicating if the function is triggered by a test client
* @param reponseChannel: channel for a connection between coordinator process and service process
 */
func queryServer(addr string, query string, isTest bool, responseChannel chan *lg.FindLogsReply) {
	tag := ""
	if isTest {
		tag = "[ TEST ]"
	}
	// Establish a TCP connection with a service process
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("%vCould not connect to node: %v", tag, addr)
	}
	defer conn.Close()
	c := lg.NewLoggerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// RPC call to the service process to fetch logs
	r, err := c.FindLogs(ctx, &lg.FindLogsRequest{Query: query, IsTest: isTest})
	if err != nil {
		// may be service process is down
		log.Printf("%vCould not connect to node: %v", tag, addr)
	}
	responseChannel <- r
}

/**
* The RPC function for querying logs
*
* @param ctx: context
* @param in: the query request
 */

func (s *CoordinatorServer) QueryLogs(ctx context.Context, in *pb.QueryRequest) (*pb.QueryReply, error) {
	query := in.GetQuery()
	isTest := in.GetIsTest()
	tag := ""

	conf := config.GetConfig("../../config/config.json")
	ml := GetMemberList()

	if isTest {
		tag = "[ TEST ]"
	}
	grepCommand := fmt.Sprintf("[ Coordinator ]grep -HEc '%v'", query)

	log.Printf("%vExecuting: %v", tag, grepCommand)

	// Establish connections with the server nodes
	responseChannel := make(chan *lg.FindLogsReply)
	numItems := 0

	// Concurrently establishing connections to all the service processes
	for memberListItem := range ml.Iter() {
		addr := (strings.Split(memberListItem.Id, ":"))[0] + fmt.Sprintf(":%d", conf.LoggerPort)
		numItems += 1
		go queryServer(addr, query, isTest, responseChannel)
	}
	logs := ""
	totalMatches := 0

	// Wait for all the service process to return the responses
	// Aggregate all the responses from service processes and redirect to the client
	for i := 0; i < numItems; i++ {
		logQueryResponse := <-responseChannel
		logs += logQueryResponse.GetLogs()
		totalMatches += int(logQueryResponse.GetNumMatches())
	}
	return &pb.QueryReply{Logs: logs, TotalMatches: int64(totalMatches)}, nil
}

/**
* Send a command to the service processes on the workers to generate logs
*
* @param addr: IP address of the worker/service process
* @param reponseChannel: channel for a connection between coordinator process and service process
* @param filenumer: file number of the test file to be generated
 */
func generateLogsOnServer(addr string, responseChannel chan *lg.GenerateLogsReply, filenumber int) {
	// Establish TCP connection with the service process
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Could not connect to node: %v", addr)
	}
	defer conn.Close()
	c := lg.NewLoggerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// RPC call to the service process to generate logs
	r, err := c.Test_GenerateLogs(ctx, &lg.GenerateLogsRequest{Filenumber: int32(filenumber)})
	if err != nil {
		log.Printf("Failed to generate Logs in: %v", addr)
	}
	responseChannel <- r
}

/**
* The RPC function for generating test logs on the service nodes
*
* @param ctx: context
* @param in: the query request
 */
func (s *CoordinatorServer) Test_GenerateLogs(ctx context.Context, in *pb.GenerateLogsRequest) (*pb.GenerateLogsReply, error) {
	// Establish connections with the server nodes
	responseChannel := make(chan *lg.GenerateLogsReply)

	// Concurrently establishing connections with service processes
	for idx, addr := range serverAddresses {
		go generateLogsOnServer(addr, responseChannel, idx+1)
	}
	status := ""

	// Wait for all the service processes to respond before aggregating response
	// and sending it to the client
	for _, addr := range serverAddresses {
		generateLogsResponse := <-responseChannel
		status += addr + ":" + generateLogsResponse.GetStatus()
	}
	return &pb.GenerateLogsReply{Status: status}, nil
}

func StartCoordinatorService(port int, devmode bool, wg *sync.WaitGroup) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterCoordinatorServer(s, &CoordinatorServer{})

	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
		wg.Done()
	}
}

func SendLogQueryRequest(coordinatorPort int, query string) {
	coordinatorIp := memberList.GetCoordinatorNode()
	if coordinatorIp == "" {
		log.Printf("No master Node\n")
		return
	}
	// start a clock to time the execution time of the querying
	start := time.Now()
	coordinatorIp = fmt.Sprintf("%s:%d", coordinatorIp, coordinatorPort)
	conn, err := grpc.Dial(coordinatorIp, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		// If the connection fails to the picked coordinator node, retry connection to another node
		log.Printf("Failed to establish connection with the coordinator....Retrying")
	}

	defer conn.Close()

	// Initialise a client to connect to the coordinator process
	c := pb.NewCoordinatorClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Call the RPC function on the coordinator process to process the query
	r, err := c.QueryLogs(ctx, &pb.QueryRequest{Query: query, IsTest: false})

	if err != nil {
		// If the connection fails to the picked coordinator node, retry connection to another node
		log.Printf("Failed to establish connection with the coordinator....Retrying")
	} else {
		// mark the current time as the end time since the processing began
		duration := time.Since(start)

		// log the result and execution time
		log.Printf("Successfully fetched logs")
		fmt.Printf(r.GetLogs())
		log.Printf("Total Matches: %v", r.GetTotalMatches())
		log.Printf("\nExecution duration: %v", duration)

	}
}
