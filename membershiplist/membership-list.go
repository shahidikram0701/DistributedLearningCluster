package membershiplist

import (
	"fmt"
	"sync"
)

type NodeState int

const (
	Alive NodeState = iota
	Suspicious
	Failed
)

type MembershipList struct {
	sync.RWMutex
	items []MembershipListItem
}

type MembershipListItem struct {
	Id                string
	State             NodeState
	IncarnationNumber int
}

func (memListItem MembershipListItem) String() string {
	return fmt.Sprintf("[%s:::%v:::%d]", memListItem.Id, memListItem.State, memListItem.IncarnationNumber)
}

func (nodeState NodeState) String() string {
	switch nodeState {
	case Alive:
		return "Alive"
	case Suspicious:
		return "Suspicious"
	case Failed:
		return "Failed"
	default:
		return fmt.Sprintf("%d", int(nodeState))
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
func (ml *MembershipList) Append(item MembershipListItem) {
	ml.Lock()
	defer ml.Unlock()

	ml.items = append(ml.items, item)
}

// Iterates over the items in the membership list
// Each item is sent over a channel, so that
// we can iterate over the slice using the builin range keyword
func (ml *MembershipList) Iter() <-chan MembershipListItem {
	c := make(chan MembershipListItem)

	f := func() {
		ml.Lock()
		defer ml.Unlock()
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
