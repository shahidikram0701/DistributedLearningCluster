package membershiplist

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type NodeStatus int

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
	ml.Lock()
	defer ml.Unlock()

	ml.items = append(ml.items, item)

	indexOfInsertedItem := len(ml.items) - 1

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
	ml.RLock()
	defer ml.RUnlock()

	return ml.items
}

func (ml *MembershipList) String() string {
	return fmt.Sprintf("%v", ml.items)
}

func (ml *MembershipList) Get(index int) *MembershipListItem {
	ml.RLock()
	defer ml.RUnlock()

	len := len(ml.items)
	i := index

	if index < 0 {
		i = (len - (index * -1)) % len
	} else if index >= len {
		i = index % len
	}

	return &ml.items[i]
}

func (ml *MembershipList) Clean() {
	log.Printf("Deleting processes that Failed and the ones that Left voluntarily")
	log.Printf("Current MembershipList: \n%v", ml.items)
	var newItems []MembershipListItem
	for _, item := range ml.items {
		if item.State.Status == Alive || item.State.Status == Suspicious {
			newItems = append(newItems, item)
		} else {
			log.Printf("Deleting: %v", item)
		}
	}

	ml.Lock()
	ml.items = newItems
	ml.Unlock()

	log.Printf("Cleaned Membership List")
	log.Printf("Current MembershipList: \n%v", ml.items)

}
