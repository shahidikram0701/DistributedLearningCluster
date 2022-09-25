package process

import (
	ml "cs425/mp/membershiplist"
	"cs425/mp/topology"
	"cs425/mp/util"
	"encoding/json"
	"fmt"
	"log"
	"net"
)

// var exitC = make(chan bool)

func SendPing(getNode func() topology.Node, network_topology *topology.Topology, memberList *ml.MembershipList) {
	nodeToPing := getNode()

	if (nodeToPing == topology.Node{}) {
		log.Printf("No node to ping\n")
		return
	}
	if memberList == nil {
		log.Printf("memberList is not initialised")
		return
	}
	pingSendingNode := network_topology.GetSelfNodeId()

	memberList.UpdateSelfIncarnationNumber(pingSendingNode)

	ip, port := nodeToPing.GetUDPAddrInfo()
	log.Printf("[ UDP Client ]Pinging %v:%v\n", ip, port)
	service := fmt.Sprintf("%s:%d", ip, port)
	RemoteAddr, err := net.ResolveUDPAddr("udp", service)

	conn, err := net.DialUDP("udp", nil, RemoteAddr)
	if err != nil {
		log.Printf("[ UDP Client ]Error dialing UDP\n%v\n", err)
	}

	log.Printf("[ UDP Client ]Established connection to %s \n", service)
	// log.Printf("[ UDP Client ][ UDP Client ]Remote UDP address : %s \n", conn.RemoteAddr().String())
	// log.Printf("[ UDP Client ]Local UDP client address : %s \n", conn.LocalAddr().String())

	defer conn.Close()

	args := make([]interface{}, 0)
	rpcbase := &util.RPCBase{
		MethodName: "Ping",
	}
	// serialisedMemberList, _ := json.Marshal(memberList.GetList())
	args = append(args, "Ping")
	rpcbase.Args = args

	toSend, err := json.Marshal(rpcbase)
	if err != nil {
		log.Printf("[ UDP Client ]Error marshalling the udp packet\n%v\n", err)

	}

	// fmt.Println("\n\n\n\n", toSend, "\n\n\n\n", "")

	message := []byte(string(toSend))

	_, err = conn.Write(message)

	if err != nil {
		log.Printf("[ UDP Client ]Errorrr: %v\n" + err.Error())
	}

	// receive message from server
	buffer := make([]byte, 4096)
	// n, addr, err := conn.ReadFromUDP(buffer)

	n, _, err := conn.ReadFromUDP(buffer)

	var response util.ResponseRPC
	err = json.Unmarshal(buffer[:n], &response)
	if err != nil {
		log.Printf("[ UDP Client ]Error Unmarshaling response\n%v\n", err)
	}
	// fmt.Println("ITERATION ", i)
	// fmt.Println("UDP Server : ", addr)

	var membershipList []ml.MembershipListItem
	unmarshallingError := json.Unmarshal([]byte(response.Response), &membershipList)
	if unmarshallingError != nil {
		log.Printf("Error while unmarshalling the membershipList\n%v", unmarshallingError)
		memberList.MarkSus(nodeToPing.GetId())
	} else {
		memberList.Merge(membershipList)
		fmt.Printf("[UDP Client] DONE MERGING")
	}
}
