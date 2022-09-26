package process

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	ml "cs425/mp/membershiplist"
	intro_proto "cs425/mp/proto/introducer_proto"
	lg "cs425/mp/proto/logger_proto"
	topology "cs425/mp/topology"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type server struct {
	lg.UnimplementedLoggerServer
}

var (
	network_topology *topology.Topology
	memberList       *ml.MembershipList
)

var (
	T_PING_SECOND = 0.5 // second
)

/**
* Get the membership list at this process
 */
func GetMemberList() *ml.MembershipList {
	return memberList
}

/**
* Get the network topology of this process
 */
func GetNetworkTopology() *topology.Topology {
	return network_topology
}

/**
* Bootstrapping the process
 */
func Run(port int, udpserverport int, log_process_port int, wg *sync.WaitGroup, introAddr string, devmode bool) {
	if !devmode {
		go StartLogServer(log_process_port, wg)
	}
	// Join the network. Get membership list from Introducer and update topology
	go JoinNetwork(introAddr, port, udpserverport, wg)

	// Start the UDP server to listen to pings and pongs
	go StartUdpServer(GetMemberList, udpserverport, wg)
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
	logFilePath = "../../../logs/*.log"
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

/**
* Start the log server which listens to requests to fetch logs
 */
func StartLogServer(port int, wg *sync.WaitGroup) {
	// service process listening to incoming tcp connections
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	lg.RegisterLoggerServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
		wg.Done()
	}
}

/**
* Get process's outbound address to add to membership list
 */
func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Printf("Couldn't get the IP address of the process\n%v", err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

/**
* Issue a request to the introducer to join the network
 */
func JoinNetwork(introducerAddress string, newProcessPort int, udpserverport int, wg *sync.WaitGroup) {
	var conn *grpc.ClientConn
	var err error
	log.Printf("Establishing connection to introducer process at %v", introducerAddress)
	conn, err = grpc.Dial(introducerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to establish connection with the introducer\n%v", err)
	}

	defer conn.Close()

	// Initialise a client to connect to the introducer process
	c := intro_proto.NewIntroducerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Call the RPC function on the introducer process to join the network
	r, err := c.Introduce(ctx, &intro_proto.IntroduceRequest{
		Ip:            fmt.Sprintf("%v", GetOutboundIP()),
		Port:          int64(newProcessPort),
		Timestamp:     fmt.Sprintf("%d", time.Now().Nanosecond()),
		Udpserverport: int64(udpserverport),
	})

	if err != nil {
		// If the connection fails to the picked coordinator node, retry connection to another node
		log.Printf("Failed to establish connection with the introducer\n%v", err)
	} else {
		var membershipList []ml.MembershipListItem
		unmarshallingError := json.Unmarshal(r.MembershipList, &membershipList)
		if unmarshallingError != nil {
			log.Printf("Error while unmarshalling the membershipList: %v\n", unmarshallingError)
		}

		log.Printf("Received Membership List from the introducer\n")
		memberList = ml.NewMembershipList(membershipList)
		myIndex := int(r.Index)
		myId := r.ProcessId
		log.Printf("Intialised self memberList\n")

		network_topology = topology.InitialiseTopology(myId, myIndex, udpserverport)
		log.Printf("Initialised Topology: %v\n", network_topology)
		var exited = make(chan bool)

		log.Printf("Starting the topology stabilisation\n")
		go network_topology.StabiliseTheTopology(wg, memberList)

		log.Printf("Starting the SWIM ping-pongs\n")
		go SendPings(wg, network_topology, memberList)

		<-exited

	}
	wg.Done()
}

// Pings the neighbours every T_PING_SECOND for failure detection
func SendPings(wg *sync.WaitGroup, network_topology *topology.Topology, memberList *ml.MembershipList) {
	ticker := time.NewTicker(time.Duration(T_PING_SECOND*1000) * time.Millisecond)
	quit := make(chan struct{})

	go func() {
		getNodeToPing := getWhichNeighbourToPing(network_topology)
		for {
			select {
			case <-ticker.C:
				SendPing(getNodeToPing, network_topology, memberList)
			case <-quit:
				ticker.Stop()
				wg.Done()
				return
			}
		}
	}()
}

/**
* Get which of the predecessor, successor or super successor
* is to be pinged in the current protocol period
 */
func getWhichNeighbourToPing(network_topology *topology.Topology) func() topology.Node {
	i := -1

	return func() topology.Node {
		// Minimum number of nodes needed for neighbours = 3
		// If the number of processes joined so far is less than 3
		// consider that for looping over the neighbours
		i = (i + 1) % int(math.Min(3, float64(network_topology.GetNumberOfNodes())))

		switch i {
		case 0:
			predecessor := network_topology.GetPredecessor()
			log.Printf("Piging predecessor: %v\n", predecessor)
			return predecessor
		case 1:
			successor := network_topology.GetSuccessor()
			log.Printf("Piging successor: %v\n", successor)
			return successor
		case 2:
			supersuccessor := network_topology.GetSuperSuccessor()
			log.Printf("Piging supersuccessor: %v\n", supersuccessor)
			return supersuccessor
		}

		return network_topology.GetPredecessor()
	}
}

/**
* Called when a process wants to leave the cluster. A delay is given to ensure
* membership list propagation to all nodes
 */

func LeaveNetwork() {
	log.Printf("Leaving Network\n")
	me := network_topology.GetSelfNodeId()
	memberList.MarkLeave(me)
	time.Sleep(3 * time.Second)
	os.Exit(3)
}
