package process

import (
	"cs425/mp/util"
	"encoding/json"
	"fmt"
	"log"
	"net"
)

// var exitC = make(chan bool)

func SendPing(toIP string, toPort int) {
	service := fmt.Sprintf("%s:%d", toIP, toPort)
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
	buffer := make([]byte, 1024)
	// n, addr, err := conn.ReadFromUDP(buffer)

	n, _, err := conn.ReadFromUDP(buffer)

	var response util.ResponseRPC
	err = json.Unmarshal(buffer[:n], &response)
	if err != nil {
		log.Printf("[ UDP Client ]Error Unmarshaling response\n%v\n", err)
	}
	// fmt.Println("ITERATION ", i)
	// fmt.Println("UDP Server : ", addr)
	log.Printf("[ UDP Client ]Received from UDP server : %v\n", response.Response)

	// exitC <- true
}
