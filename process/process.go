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
	memberList       *ml.MembershipList
	network_topology *topology.Topology
)

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

func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Printf("Couldn't get the IP address of the process\n%v", err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

// Handlers of Failure detection
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
			log.Printf("Error while unmarshalling the membershipList\n%v", unmarshallingError)
		}
		log.Printf("Received Membership List from the introducer\n")
		log.Printf("Intialising self memberList")
		memberList = ml.NewMembershipList(membershipList)
		myIndex := int(r.Index)
		myId := r.ProcessId

		log.Printf("Initialising Topology")
		network_topology = topology.InitialiseTopology(myId, myIndex, udpserverport)

		log.Printf("Starting the topology stabilisation")
		network_topology.StabiliseTheTopology(wg, memberList)

		SendPings(wg, network_topology)

	}
	wg.Done()
}

func SendPings(wg *sync.WaitGroup, network_topology *topology.Topology) {
	ticker := time.NewTicker(1 * time.Second)
	quit := make(chan struct{})
	go func() {
		getNodeToPing := getWhichNeighbourToPing(network_topology)
		for {
			select {
			case <-ticker.C:
				fmt.Println("\n\nPINGGGGGGGG\n\n", "")
				sendPing(getNodeToPing)
				// close(quit)
			case <-quit:
				ticker.Stop()
				wg.Done()
				return
			}
		}
	}()
}

func sendPing(getNode func() topology.Node) {
	nodeToPing := getNode()

	if (nodeToPing == topology.Node{}) {
		log.Printf("No node to ping\n")
		return
	}
	ip, port := nodeToPing.GetUDPAddrInfo()

	log.Printf("Sending Ping to %v:%v\n", ip, port)
	SendPing(ip, port)
	log.Printf("Sent Ping to %v:%v\n", ip, port)

}

func getWhichNeighbourToPing(network_topology *topology.Topology) func() topology.Node {
	i := -1

	return func() topology.Node {
		i = (i + 1) % int(math.Min(3, float64(network_topology.GetNumberOfNodes())))

		// after one iteration, shuffle the order
		// right now its always predecessor (0), successor (1) and superSuccessor (2)

		switch i {
		case 0:
			return network_topology.GetPredecessor()
		case 1:
			return network_topology.GetSuccessor()
		case 2:
			return network_topology.GetSuperSuccessor()
		}

		return network_topology.GetPredecessor()
	}
}

func getId() {
}

func runFailureDetector() {
}
