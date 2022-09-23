package process

import (
	"fmt"
	"log"
	"sync"
)

type Handler int

var exitS = make(chan bool)

func (h *Handler) Ping() string {
	return "Pong"
}

func StartUdpServer(port int, wg *sync.WaitGroup) {
	var h Handler
	server := NewServer(h, fmt.Sprintf(":%v", port))
	// listen to incoming udp packets
	var exited = make(chan bool)
	go server.ListenServer(exited)
	log.Printf("[UDP Server]server listening at :%v", port)

	if s := <-exited; s {
		// Handle Error in method
		log.Printf("[UDP server]We get an error listen server")
		return
	}
	<-exitS
	wg.Done()
}
