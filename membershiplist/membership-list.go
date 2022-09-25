package membershiplist

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type NodeStatus int

var (
	T_FAIL   = 1 // 2 second
	T_DELETE = 1 // 2 second
	T_LEAVE  = 1 // 1 second
)

const (
	Alive NodeStatus = iota
	Suspicious
	Failed
	Delete
	Left
)

type MembershipList struct {
	sync.RWMutex
	items []MembershipListItem
}

type NodeState struct {
	Status    NodeStatus
	Timestamp time.Time
}

type MembershipListItem struct {
	Id                string
	State             NodeState
	IncarnationNumber int
	UDPPort           int
}

func (memListItem MembershipListItem) String() string {
	return fmt.Sprintf("[%s:::%v:::%d]", memListItem.Id, memListItem.State, memListItem.IncarnationNumber)
}

func (nodeState NodeState) String() string {
	return fmt.Sprintf("%v at %v", nodeState.Status, nodeState.Timestamp)
}

func (nodeStatus NodeStatus) String() string {
	switch nodeStatus {
	case Alive:
		return "Alive"
	case Suspicious:
		return "Suspicious"
	case Failed:
		return "Failed"
	case Delete:
		return "Delete"
	case Left:
		return "Left"
	default:
		return fmt.Sprintf("%d", int(nodeStatus))
	}
}

// NewMembershipList creates a new concurrent Membership list
func NewMembershipList(listItems ...([]MembershipListItem)) *MembershipList {
	var cs *MembershipList
	if len(listItems) == 0 {
		cs = &MembershipList{
			items: make([]MembershipListItem, 0),
		}
	} else {
		cs = &MembershipList{
			items: listItems[0],
		}
	}

	return cs
}

// Appends an item to the membership list
func (ml *MembershipList) Append(item MembershipListItem) int {
	fmt.Printf("\n\n[Acquire LOCK]<MembershipList.Append>\n\n")
	ml.Lock()
	defer ml.Unlock()

	fmt.Printf("\n\n[LOCK]<MembershipList.Append>\n\n")

	ml.items = append(ml.items, item)

	indexOfInsertedItem := len(ml.items) - 1

	fmt.Printf("\n\n[Release LOCK]<MembershipList.Append>\n\n")
	return indexOfInsertedItem
}

// Iterates over the items in the membership list
// Each item is sent over a channel, so that
// we can iterate over the slice using the builin range keyword
func (ml *MembershipList) Iter() <-chan MembershipListItem {
	c := make(chan MembershipListItem)

	f := func() {
		ml.RLock()
		defer ml.RUnlock()
		for _, value := range ml.items {
			c <- value
		}
		close(c)
	}
	go f()

	return c
}

// Len is the number of items in the concurrent slice.
func (ml *MembershipList) Len() int {
	ml.RLock()
	defer ml.RUnlock()

	return len(ml.items)
}

func (ml *MembershipList) GetList() []MembershipListItem {
	return ml.items
}

func (ml *MembershipList) String() string {
	return fmt.Sprintf("%v", ml.items)
}

func (ml *MembershipList) Clean() {
	fmt.Printf("\n\n[Acquire LOCK]<MembershipList.Clean>\n\n")
	log.Printf("\n\nMembershipList.Clean()\n\n")
	ml.Lock()
	defer ml.Unlock()

	fmt.Printf("\n\n[LOCK]<MembershipList.Clean>\n\n")

	log.Printf("Deleting processes that Failed and the ones that Left voluntarily")
	log.Printf("Current MembershipList: \n%v", ml.items)
	var newItems []MembershipListItem
	for _, item := range ml.items {
		if item.State.Status != Delete {
			newItems = append(newItems, item)
		} else {
			log.Printf("Deleting: %v", item)
		}
	}

	ml.items = newItems

	log.Printf("Cleaned Membership List")
	log.Printf("Current MembershipList: \n%v", ml.items)

	log.Printf("\n\nDONE: MembershipList.Clean()\n\n")

	fmt.Printf("\n\n[Release LOCK]<MembershipList.Clean>\n\n")

}

func (ml *MembershipList) UpdateSelfIncarnationNumber(myId string) {
	log.Printf("\n\nMembershipList.UpdateSelfIncarnationNumber\n\n")
	fmt.Printf("\n\n[Acquire LOCK]<MembershipList.UpdateSelfIncarnationNumber>\n\n")
	ml.Lock()
	defer ml.Unlock()

	fmt.Printf("\n\n[LOCK]<MembershipList.UpdateSelfIncarnationNumber>\n\n")

	log.Printf("Updating my (%v) Incarnation Number\n", myId)
	log.Printf("MembershipList: %v\n", ml.items)
	// fmt.Printf("Updating my (%v) Incarnation Number\n", myId)
	// fmt.Printf("MembershipList: %v\n", ml.items)
	for i := 0; i < len(ml.items); i++ {
		// fmt.Printf("item.Id = %v\n", ml.items[i].Id)
		if ml.items[i].Id == myId {
			// fmt.Printf("Match at i = %v\n\n", i)
			ml.items[i].IncarnationNumber = ml.items[i].IncarnationNumber + 1
			break
		}

	}
	log.Printf("\n\nMembershipList.UpdateSelfIncarnationNumber\n\n")
	// fmt.Printf("New MembershipList: %v\n", ml.items)

	fmt.Printf("\n\n[Release LOCK]<MembershipList.UpdateSelfIncarnationNumber>\n\n")
}

func (ml *MembershipList) MarkSus(nodeId string) {
	log.Printf("\n\nMembershipList.MarkSus\n\n")

	fmt.Printf("\n\n[Acquire LOCK]<MembershipList.MarkSus>\n\n")
	ml.Lock()
	defer ml.Unlock()

	fmt.Printf("\n\n[LOCK]<MembershipList.MarkSus>\n\n")

	log.Printf("Marking node (%v) Suspicious\n", nodeId)
	log.Printf("MembershipList: %v\n", ml.items)
	// fmt.Printf("Updating my (%v) Incarnation Number\n", myId)
	// fmt.Printf("MembershipList: %v\n", ml.items)
	for i := 0; i < len(ml.items); i++ {
		// fmt.Printf("item.Id = %v\n", ml.items[i].Id)
		if ml.items[i].Id == nodeId && ml.items[i].State.Status == Alive {
			// fmt.Printf("Match at i = %v\n\n", i)
			ml.items[i].State.Status = Suspicious
			ml.items[i].State.Timestamp = time.Now()
			break
		}

	}
	log.Printf("\n\nDONE: MembershipList.MarkSus\n\n")
	fmt.Printf("\n\n[Release LOCK]<MembershipList.MarkSus>\n\n")
}

func (m1 *MembershipList) Merge(m2 []MembershipListItem) {
	log.Printf("\n\nMembershipList.Merge\n\n")
	fmt.Printf("\n\n[Acquire LOCK]<MembershipList.Merge>\n\n")
	m1.Lock()
	defer m1.Unlock()

	fmt.Printf("\n\n[LOCK]<MembershipList.Merge>\n\n")

	log.Printf("Merging Membership lists\n")
	log.Printf("Current Membership List\n%v\v\n", m1.items)
	log.Printf("Received Membership List\n%v\v\n", m2)

	// fmt.Printf("Merging Membership lists\n")
	// fmt.Printf("Current Membership List\n%v\v", m1.items)
	// fmt.Printf("Received Membership List\n%v\v", m2)

	var newItems []MembershipListItem

	i := 0
	j := 0

	for i < len(m1.items) && j < len(m2) {
		// fmt.Printf("%v; ", i)
		m1Item := m1.items[i]
		m2Item := m2[j]

		if m1Item.Id == m2Item.Id {
			// log.Printf("here\n")
			if m1Item.State.Status == Failed || m1Item.State.Status == Delete {
				newItems = append(newItems, m1Item)
			} else {
				if (m2Item.State.Status == Failed || m2Item.State.Status == Delete) &&
					(m1Item.State.Status != m2Item.State.Status) {
					m1Item.State = m2Item.State
					m1Item.State.Timestamp = time.Now()
					newItems = append(newItems, m1Item)
				} else {
					if m2Item.IncarnationNumber > m1Item.IncarnationNumber {
						m1Item.State = m2Item.State
						m1Item.State.Timestamp = time.Now()
						m1Item.IncarnationNumber = m2Item.IncarnationNumber
						newItems = append(newItems, m1Item)
					} else {
						newItems = append(newItems, m1Item)
					}
				}
			}

			// if m2Item.IncarnationNumber > m1Item.IncarnationNumber {
			// 	m1Item.State = m2Item.State
			// 	m1Item.State.Timestamp = time.Now()
			// 	m1Item.IncarnationNumber = m2Item.IncarnationNumber
			// 	newItems = append(newItems, m1Item)

			// } else if m2Item.IncarnationNumber == m1Item.IncarnationNumber {
			// 	if m2Item.State.Status == Suspicious || m2Item.State.Status == Failed {
			// 		m1Item.State = m2Item.State
			// 		m1Item.State.Timestamp = time.Now()
			// 	}
			// } else {
			// 	if m2Item.State.Status == Failed || m2Item.State.Status == Delete {
			// 		m1Item.State = m2Item.State
			// 		m1Item.State.Timestamp = time.Now()
			// 	}
			// }

			// newItems = append(newItems, m1Item)

			i++
			j++
		} else {
			// m1Item.State.Status == Left || m1Item.State.Status == Delete || m2Item.State.Status == Failed
			// newItems = append(newItems, m1Item)
			j++
		}
	}
	// I had more list items
	for i < len(m1.items) {
		// log.Printf("here2\n")
		// m[j].State.Timestamp = time.Now()
		newItems = append(newItems, m1.items[i])
		i++
	}

	// I should know about more items
	for j < len(m2) {
		// log.Printf("here3\n")
		m2[j].State.Timestamp = time.Now()
		newItems = append(newItems, m2[j])
		j++
	}

	m1.items = newItems

	log.Printf("New Membership List: \n%v\n\n", newItems)
	// fmt.Printf("Membership List: \n%v\n\n\n", newItems)

	log.Printf("\n\nDONE: MembershipList.Merge\n\n")

	fmt.Printf("\n\n[Release LOCK]<MembershipList.Merge>\n\n")

}

func (ml *MembershipList) UpdateStates() []MembershipListItem {
	log.Printf("\n\nUpdating states of processes in Membership List\n\n")
	fmt.Printf("\n\n[Acquire LOCK]<MembershipList.UpdateStates>\n\n")
	// fmt.Printf("Updating states of processes in Membership List\n")
	ml.Lock()
	defer ml.Unlock()

	fmt.Printf("\n\n[LOCK]<MembershipList.UpdateStates>\n\n")
	// fmt.Printf("Acquried Lock")
	currentTime := time.Now().Unix()

	for i := 0; i < len(ml.items); i++ {
		// log.Printf("\n previous = %v\n", previous.Id)
		// log.Printf("\n pprevious = %v\n", pprevious.Id)
		// log.Printf("\nml.item[i]: %v\n", ml.items[i].Id)
		// log.Printf("\nself: %v\n", topo.ring.self.id)

		if ml.items[i].State.Status == Suspicious &&
			(currentTime-ml.items[i].State.Timestamp.Unix() >= int64(T_FAIL)) {
			ml.items[i].State.Status = Failed
			// ml.items[i].IncarnationNumber++
		} else if ml.items[i].State.Status == Failed &&
			(currentTime-ml.items[i].State.Timestamp.Unix() >= int64(T_DELETE)) {
			ml.items[i].State.Status = Delete
			// ml.items[i].IncarnationNumber++
		} else if ml.items[i].State.Status == Left &&
			(currentTime-ml.items[i].State.Timestamp.Unix() >= int64(T_LEAVE)) {
			ml.items[i].State.Status = Delete
			// ml.items[i].IncarnationNumber++
		}
	}

	log.Printf("\n\nDONE: Updating states of processes in Membership List\n\n")
	fmt.Printf("\n\n[Release LOCK]<MembershipList.UpdateStates>\n\n")

	return ml.items

}
