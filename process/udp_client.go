package process

import (
	"cs425/mp/util"
	"encoding/json"
	"fmt"
	"log"
	"net"
)

var exitC = make(chan bool)

func sendPing(toIP string, toPort int) {
	service := fmt.Sprintf("%s:%d", toIP, toPort)
	RemoteAddr, err := net.ResolveUDPAddr("udp", service)

	conn, err := net.DialUDP("udp", nil, RemoteAddr)
	if err != nil {
		log.Printf("Error dialing UDP\n%v\n", err)
	}

	log.Printf("Established connection to %s \n", service)
	// log.Printf("Remote UDP address : %s \n", conn.RemoteAddr().String())
	// log.Printf("Local UDP client address : %s \n", conn.LocalAddr().String())

	defer conn.Close()

	rpcbase := &util.RPCBase{
		MethodName: "Ping",
	}
	some := make([]interface{}, 0)

	rpcbase.Args = some

	toSend, err := json.Marshal(rpcbase)
	if err != nil {
		fmt.Println(err)
		return

	}

	message := []byte(string(toSend))

	for {
		_, err = conn.Write(message)

		if err != nil {
			log.Printf("Errorrr: %v\n" + err.Error())
			break
		}

		// receive message from server
		buffer := make([]byte, 1024)
		// n, addr, err := conn.ReadFromUDP(buffer)

		n, _, err := conn.ReadFromUDP(buffer)

		var response util.ResponseRPC
		err = json.Unmarshal(buffer[:n], &response)
		if err != nil {
			fmt.Println("Error Unmarshaling response")
			break
		}
		// fmt.Println("ITERATION ", i)
		// fmt.Println("UDP Server : ", addr)
		fmt.Println("Received from UDP server : ", response.Response)

	}
	exitC <- true
}
