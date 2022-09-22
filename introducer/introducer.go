package introducer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	ml "cs425/mp/membershiplist"
	intro "cs425/mp/proto/introducer_proto"

	"google.golang.org/grpc"
)

type server struct {
	intro.UnimplementedIntroducerServer
}

var (
	memberList *ml.MembershipList
)

func StartIntroducerAndListenToConnections(devmode bool, port int, wg *sync.WaitGroup) {
	introducerAddress := "shahidi3@fa22-cs425-3701.cs.illinois.edu"

	if devmode {
		introducerAddress = "localhost"
	}
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))

	memberList = ml.NewMembershipList()

	// Adding the introducer to the membership list
	memberList.Append(ml.MembershipListItem{
		Id:                fmt.Sprintf("%s:%d:%d", introducerAddress, port, time.Now().UnixNano()),
		State:             ml.Alive,
		IncarnationNumber: 0,
	})

	if err != nil {
		log.Printf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	intro.RegisterIntroducerServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Printf("failed to serve: %v", err)
		wg.Done()
	}
}

// RPC server handler
func (s *server) Introduce(ctx context.Context, in *intro.IntroduceRequest) (*intro.IntroduceReply, error) {
	requestorIP := in.Ip
	requestorPort := in.Port
	requestorTimestamp := in.Timestamp

	// create an id for the new process
	newProcessId := fmt.Sprintf("%s:%d:%s", requestorIP, requestorPort, requestorTimestamp)

	log.Printf("Introducing %s to the system", newProcessId)

	// Adding the new process to Introducer's membership list
	memberList.Append(ml.MembershipListItem{
		Id:                newProcessId,
		State:             ml.Alive,
		IncarnationNumber: 0,
	})

	// Introducer needs to send the complete membership list to the new node
	log.Printf("Updated membership list: %v", memberList)

	reply := intro.IntroduceReply{}
	if serialisedMemberList, err := json.Marshal(memberList.GetList()); err == nil {
		reply.MembershipList = serialisedMemberList
	} else {
		log.Printf("Error Marshalling the membership list to be sent\n%v", err)
	}

	return &reply, nil
}