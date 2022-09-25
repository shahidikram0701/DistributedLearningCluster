package topology

import (
	ml "cs425/mp/membershiplist"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

type topologyInner struct {
	self           Node
	predecessor    Node
	successor      Node
	superSuccessor Node

	numberOfProcesses int
}

type Node struct {
	index         int
	id            string
	udpserverport int
}

type Topology struct {
	sync.RWMutex
	ring   topologyInner
	selfId string
}

var (
	T_STABILISE = 1 // 10 second
)

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

func (node *Node) GetUDPAddrInfo() (string, int) {
	splitId := strings.Split(node.id, ":")
	// fmt.Println(splitId)

	return splitId[0], node.udpserverport
}

func (node *Node) GetId() string {

	return node.id
}

func (topo *Topology) String() string {
	return fmt.Sprintf("%v - %v - %v - %v\n",
		topo.ring.predecessor.id,
		topo.ring.self.id,
		topo.ring.successor.id,
		topo.ring.superSuccessor.id,
	)
}

func (topo *Topology) GetSelfNodeId() string {
	return topo.selfId
}

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

func stabiliseTopology(topo *Topology, memberList *ml.MembershipList) {

	list := memberList.UpdateStates()

	log.Printf("Updated States in MembershipList\n%v\n", memberList)

	// memberListLength := memberList.Len()

	// pprevious := memberList.Get(memberListLength - 2)
	// previous := memberList.Get(memberListLength - 1)
	i := 0
	// fmt.Printf("\n\n[Acquire LOCK]<Topology.stabiliseTopology>\n\n")
	topo.Lock()

	// fmt.Printf("\n\n[LOCK]<Topology.stabiliseTopology>\n\n")

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
	// fmt.Printf("\n\n[Released LOCK]<Topology.stabiliseTopology>\n\n")
	memberList.Clean()
	topo.ring.numberOfProcesses = i
}

func (topo *Topology) updateSelfIndex(newIndex int) {
	// fmt.Printf("\n\n[Acquire LOCK]<Topology.updateSelfIndex>\n\n")
	topo.Lock()
	defer topo.Unlock()
	// fmt.Printf("\n\n[LOCK]<Topology.updateSelfIndex>\n\n")
	topo.ring.self.index = newIndex

	// fmt.Printf("\n\n[Release LOCK]<Topology.updateSelfIndex>\n\n")
}

func (topo *Topology) GetPredecessor() Node {
	topo.RLock()
	defer topo.RUnlock()

	return topo.ring.predecessor
}

func (topo *Topology) GetSuccessor() Node {
	topo.RLock()
	defer topo.RUnlock()

	return topo.ring.successor
}

func (topo *Topology) GetSuperSuccessor() Node {
	topo.RLock()
	defer topo.RUnlock()

	return topo.ring.superSuccessor
}

func (topo *Topology) GetNeighbors() (Node, Node, Node) {
	topo.RLock()
	defer topo.RUnlock()

	return topo.ring.predecessor, topo.ring.successor, topo.ring.superSuccessor
}

func (topo *Topology) GetNumberOfNodes() int {
	topo.RLock()
	defer topo.RUnlock()

	return topo.ring.numberOfProcesses
}

func (topo *Topology) ClearTopology() {
	topo.Lock()
	defer topo.Unlock()

	topo.ring.predecessor = Node{}
	topo.ring.successor = Node{}
	topo.ring.superSuccessor = Node{}

}
