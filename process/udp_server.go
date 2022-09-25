package process

import (
	"fmt"
	"log"
	"sync"

	ml "cs425/mp/membershiplist"
)

type Handler int

var exitS = make(chan bool)

func (h *Handler) Ping(memberList string) string {
	// log.Printf("[ UDP Server ]Handling Ping")

	log.Printf("\n\nPONG: %v\n\n", memberList)

	return memberList
}

func StartUdpServer(getMembershipList func() *ml.MembershipList, port int, wg *sync.WaitGroup) {
	var h Handler
	server := NewServer(h, fmt.Sprintf(":%v", port))

	// log.Printf("[UDP Server]MembershipList: %v\n", getMembershipList())
	// listen to incoming udp packets
	var exited = make(chan bool)
	go server.ListenServer(exited, getMembershipList())
	log.Printf("[UDP Server]server listening at :%v", port)

	if s := <-exited; s {
		// Handle Error in method
		log.Printf("[UDP server]We get an error listen server")
		return
	}
	<-exitS
	wg.Done()
}
