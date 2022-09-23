package topology

import (
	ml "cs425/mp/membershiplist"
	"log"
	"sync"
	"time"
)

var (
	T_FAIL      = 2 // 2 second
	T_DELETE    = 2 // 2 second
	T_LEAVE     = 1 // 1 second
	T_STABILISE = 2 // 2 second
)

type topologyInner struct {
	self           node
	predecessor    node
	successor      node
	superSuccessor node
}

type node struct {
	index int
	id    string
}

type Topology struct {
	sync.RWMutex
	ring topologyInner
}

func InitialiseTopology(selfId string, index int) *Topology {
	var topology *Topology
	topology = new(Topology)
	topology.ring.self.id = selfId

	log.Printf("Initialised Topology: %v", topology)

	return topology
}

func (topo *Topology) StabiliseTheTopology(wg *sync.WaitGroup, memberList *ml.MembershipList) {
	ticker := time.NewTicker(time.Duration(T_STABILISE) * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				topo.Lock()
				stabiliseTopology(topo, memberList)
				log.Printf("Topology:\n%v - %v - %v - %v\n",
					topo.ring.predecessor.id,
					topo.ring.self.id,
					topo.ring.successor.id,
					topo.ring.superSuccessor.id,
				)
				topo.Unlock()
				// close(quit)
			case <-quit:
				ticker.Stop()
				wg.Done()
				return
			}
		}
	}()
}

func stabiliseTopology(topo *Topology, memberList *ml.MembershipList) {
	log.Printf("Stabilising Topology")
	// log.Printf("MembershipList\n%v\n", memberList)

	currentTime := time.Now().Unix()
	// memberListLength := memberList.Len()

	// pprevious := memberList.Get(memberListLength - 2)
	// previous := memberList.Get(memberListLength - 1)
	i := 0
	for value := range memberList.Iter() {
		// log.Printf("\n previous = %v\n", previous.Id)
		// log.Printf("\n pprevious = %v\n", pprevious.Id)
		// log.Printf("\nvalue: %v\n", value.Id)
		// log.Printf("\nself: %v\n", topo.ring.self.id)

		if value.State.Status == ml.Suspicious && (currentTime-value.State.Timestamp.Unix() >= int64(T_FAIL)) {
			value.State.Status = ml.Failed
			value.IncarnationNumber++
		} else if value.State.Status == ml.Failed && (currentTime-value.State.Timestamp.Unix() >= int64(T_DELETE)) {
			value.State.Status = ml.Delete
			value.IncarnationNumber++
		} else if value.State.Status == ml.Left && (currentTime-value.State.Timestamp.Unix() >= int64(T_LEAVE)) {
			value.State.Status = ml.Delete
			value.IncarnationNumber++
		}

		if value.Id == topo.ring.self.id {
			topo.ring.predecessor.id = memberList.Get(i - 1).Id
			topo.ring.successor.id = memberList.Get(i + 1).Id
			topo.ring.superSuccessor.id = memberList.Get(i + 2).Id
		}

		// if value.Id == topo.ring.self.id {
		// 	log.Printf("updated predecessor")
		// 	topo.ring.predecessor.id = previous.Id
		// }
		// if previous.Id == topo.ring.self.id {
		// 	log.Printf("udpated successor")
		// 	topo.ring.successor.id = value.Id
		// }
		// if pprevious.Id == topo.ring.self.id {
		// 	log.Printf("updated supersuccessor")
		// 	topo.ring.superSuccessor.id = value.Id
		// }

		// pprevious = previous
		// previous = &value
		i++
	}

	memberList.Clean()
}

func (topo *Topology) updateSelfIndex(newIndex int) {
	topo.Lock()
	defer topo.Unlock()

	topo.ring.self.index = newIndex
}

func (topo *Topology) getNeighbors() (node, node, node) {
	topo.Lock()
	defer topo.Unlock()

	return topo.ring.predecessor, topo.ring.successor, topo.ring.superSuccessor
}
