package process

import (
	"fmt"
)

type Handler int

var exitS = make(chan bool)

func (h *Handler) Ping(i float64, j string) string {
	fmt.Println("i -> ", i)
	fmt.Println("j -> ", j)

	return "Pong"
}

func StartUdpServer() {
	var h Handler
	server := NewServer(h, ":1053")
	// listen to incoming udp packets
	var exited = make(chan bool)
	go server.ListenServer(exited)
	if s := <-exited; s {
		// Handle Error in method
		fmt.Println("We get an error listen server")
		return
	}
	<-exitS
}
