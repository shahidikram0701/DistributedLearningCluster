package topology

import (
	ml "cs425/mp/membershiplist"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

// Type of the topology ring
type topologyInner struct {
	self           Node
	predecessor    Node
	successor      Node
	superSuccessor Node

	// Holds the number of processes in the membership list
	numberOfProcesses int
}

// Type of the each process
type Node struct {
	index         int
	id            string
	udpserverport int
}

// Type of the topology
type Topology struct {
	sync.RWMutex
	ring   topologyInner
	selfId string
}

// TIME interval to continuously run stabilise protocol
var (
	T_STABILISE = 1 // 10 second
)

/**
* Initialise the topology for the process
* Mark the predecessor, successor and supersuccessor of the process
 */
func InitialiseTopology(selfId string, index int, udpserverport int) *Topology {
	var topology *Topology
	topology = new(Topology)

	topology.selfId = selfId

	topology.ring.self.id = selfId
	topology.ring.numberOfProcesses = 1
	topology.ring.self.udpserverport = udpserverport

	log.Printf("Initialised Topology: %v", topology)

	return topology
}

/**
* Method to get the address of the UDP endpoint to ping
 */
func (node *Node) GetUDPAddrInfo() (string, int) {
	splitId := strings.Split(node.id, ":")
	return splitId[0], node.udpserverport
}

/**
* Get the node id
 */
func (node *Node) GetId() string {
	return node.id
}

/**
* Method overrding the String method
 */
func (topo *Topology) String() string {
	return fmt.Sprintf("%v - %v - %v - %v\n",
		topo.ring.predecessor.id,
		topo.ring.self.id,
		topo.ring.successor.id,
		topo.ring.superSuccessor.id,
	)
}

/**
* Get current process's id
 */
func (topo *Topology) GetSelfNodeId() string {
	return topo.selfId
}

/**
* Runs every T_STABILISE (10s)
* This thread iterates through the membership list and adjusts the topology
 */
func (topo *Topology) StabiliseTheTopology(wg *sync.WaitGroup, memberList *ml.MembershipList) {
	ticker := time.NewTicker(time.Duration(T_STABILISE) * time.Second)
	quit := make(chan struct{})
	func() {
		for {
			select {
			case <-ticker.C:

				log.Printf("Stabilising Topology\n")
				stabiliseTopology(topo, memberList)
				log.Printf("Topology:\n%v - %v - %v - %v\n",
					topo.ring.predecessor.id,
					topo.ring.self.id,
					topo.ring.successor.id,
					topo.ring.superSuccessor.id,
				)
				// close(quit)
			case <-quit:
				log.Printf("Stopped Stabilising topology")
				ticker.Stop()
				wg.Done()
				return
			}
		}
	}()
}

/**
* Get item at a particular index in the membership list in a cyclic way
 */
func get(items []ml.MembershipListItem, index int) *ml.MembershipListItem {
	len := len(items)
	i := index

	if index < 0 {
		i = (len - (index * -1)) % len
	} else if index >= len {
		i = index % len
	}

	return &items[i]
}

/**
* Procedure responsible for updating the neighbours of each node in the membership list
 */
func stabiliseTopology(topo *Topology, memberList *ml.MembershipList) {
	list := memberList.UpdateStates()
	log.Printf("Updated States in MembershipList\n%v\n", memberList)
	i := 0
	topo.Lock()

	for _, value := range list {
		if value.Id == topo.ring.self.id {
			pred := get(list, i-1)
			succ := get(list, i+1)
			ssucc := get(list, i+2)
			topo.ring.predecessor.id, topo.ring.predecessor.udpserverport = pred.Id, pred.UDPPort
			topo.ring.successor.id, topo.ring.successor.udpserverport = succ.Id, succ.UDPPort
			topo.ring.superSuccessor.id, topo.ring.superSuccessor.udpserverport = ssucc.Id, ssucc.UDPPort
		}
		i++
	}
	topo.Unlock()
	memberList.Clean()
	topo.ring.numberOfProcesses = i
}

/**
* Update the index of self process
 */
func (topo *Topology) updateSelfIndex(newIndex int) {
	topo.Lock()
	defer topo.Unlock()
	topo.ring.self.index = newIndex
}

/**
* Method to access the predecessor
 */
func (topo *Topology) GetPredecessor() Node {
	topo.RLock()
	defer topo.RUnlock()

	return topo.ring.predecessor
}

/**
* Method to access the Successor
 */
func (topo *Topology) GetSuccessor() Node {
	topo.RLock()
	defer topo.RUnlock()

	return topo.ring.successor
}

/**
* Method to access the Super Sucessor
 */
func (topo *Topology) GetSuperSuccessor() Node {
	topo.RLock()
	defer topo.RUnlock()

	return topo.ring.superSuccessor
}

/**
* Method to access the neighbors of the current process from the topology
 */
func (topo *Topology) GetNeighbors() (Node, Node, Node) {
	topo.RLock()
	defer topo.RUnlock()

	return topo.ring.predecessor, topo.ring.successor, topo.ring.superSuccessor
}

/**
* Method to get the current number of nodes in the ring
 */
func (topo *Topology) GetNumberOfNodes() int {
	topo.RLock()
	defer topo.RUnlock()

	return topo.ring.numberOfProcesses
}

/**
* Method to clear the topology
 */
func (topo *Topology) ClearTopology() {
	topo.Lock()
	defer topo.Unlock()

	topo.ring.predecessor = Node{}
	topo.ring.successor = Node{}
	topo.ring.superSuccessor = Node{}

}
