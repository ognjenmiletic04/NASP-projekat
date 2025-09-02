package memtable

import (
	"fmt"
	"math/rand"
	"project/blockmanager"
)

type SkipList struct {
	maxHeight       int
	currentCapacity int
	head            *Node
}

func NewSkipList(maxHeight int) *SkipList {

	// Kreiranje head i tail čvorova za najviši nivo (maxHeight - 1)
	minString := []byte("")
	maxString := []byte("")
	headKey := ""                 // Najmanji mogući ključ (prazan string)
	tailKey := "\xFF\xFF\xFF\xFF" // Najveći mogući ključ

	headRecord := blockmanager.SetRec(0, 0, 0, uint64(len(headKey)), uint64(len(minString)), headKey, minString)
	tailRecord := blockmanager.SetRec(0, 0, 0, uint64(len(tailKey)), uint64(len(maxString)), tailKey, maxString)
	head := &Node{record: headRecord, level: maxHeight - 1, next: nil, below: nil}
	tail := &Node{record: tailRecord, level: maxHeight - 1, next: nil, below: nil}
	head.next = tail

	currentHead := head
	currentTail := tail

	// Kreiranje head i tail čvorova za sve nivoe ispod najvišeg
	// Postavljanje `below` i `next`
	for i := maxHeight - 2; i >= 0; i-- {
		belowHeadRecord := blockmanager.SetRec(0, 0, 0, uint64(len(headKey)), uint64(len(minString)), headKey, minString)
		belowTailRecord := blockmanager.SetRec(0, 0, 0, uint64(len(tailKey)), uint64(len(maxString)), tailKey, maxString)
		belowHead := &Node{record: belowHeadRecord, level: i, next: nil, below: nil}
		belowTail := &Node{record: belowTailRecord, level: i, next: nil, below: nil}
		belowHead.next = belowTail

		// Povezivanje trenutnih head i tail čvorova sa onima ispod
		currentHead.below = belowHead
		currentTail.below = belowTail

		// Ažuriranje trenutnih čvorova
		currentHead = belowHead
		currentTail = belowTail
	}

	return &SkipList{maxHeight: maxHeight, currentCapacity: 0, head: head}
}

func (s *SkipList) roll() int {
	level := 0
	// possible ret values from rand are 0 and 1
	// we stop shen we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight-1 {
			return level
		}
	}
	return level
}

func (s *SkipList) Find(key string) *Node {
	current := s.head

	for current != nil {
		if current.next != nil && current.next.record.GetKey() < key {
			current = current.next
		} else if current.next != nil && current.next.record.GetKey() == key {
			return current.next
		} else if current.below != nil {
			current = current.below
		} else {
			break
		}
	}
	return nil

}

// Funkcija za dodavanje cvora u SkipList
func (s *SkipList) Insert(record *blockmanager.Record) {
	current := s.head
	// Lista cvorova koji su prethodnici cvora koji se dodaje
	prevNodes := make([]*Node, s.maxHeight)
	level := s.maxHeight - 1

	// prolazak kroz listu dok ne nadjemo mesto za cvor
	for current != nil {
		if current.next != nil && current.next.record.GetKey() < record.GetKey() {
			current = current.next
		} else {
			prevNodes[level] = current
			if current.below != nil {
				current = current.below
				level--
			} else {
				break
			}
		}
	}

	//Bacamo novcic da dobijemo nivo cvora
	newNodeLevel := s.roll()
	if newNodeLevel >= s.maxHeight {
		newNodeLevel = s.maxHeight - 1
	}

	//Pocetna inicijalizacija cvora ispod trenutnog cvora na nil
	var belowNode *Node
	for i := 0; i <= newNodeLevel; i++ {

		// Kreiranje novog cvora za svaki nivo
		newNode := &Node{record: record, level: i, next: nil, below: belowNode}
		//Prevezivanje pokaživaca
		prevNode := prevNodes[i]
		newNode.next = prevNode.next
		prevNode.next = newNode
		// Postavljanje cvora ispod trenutnog cvora
		belowNode = newNode
	}
	s.currentCapacity++
}

// Funkcija za brisanje cvora iz SkipList
func (s *SkipList) Delete(key string) {
	current := s.head
	prevNodes := make([]*Node, s.maxHeight)
	level := s.maxHeight - 1

	// Pronalazak pozicije na kojoj bi se cvor nalazio
	for current != nil {
		if current.next != nil && current.next.record.GetKey() < key {
			current = current.next
		} else {
			prevNodes[level] = current
			if current.below != nil {
				current = current.below
				level--
			} else {
				break
			}
		}
	}

	// Brisanje cvora sa svih nivoa ako taj cvor postoji
	if current.next != nil && current.next.record.GetKey() == key {
		for i := 0; i < s.maxHeight; i++ {
			if prevNodes[i].next != nil && prevNodes[i].next.record.GetKey() == key {
				prevNodes[i].next = prevNodes[i].next.next
			}
		}
	}
}

// Funkcija za ispisivanje SkipList
func (s *SkipList) Print() {
	for level := s.maxHeight - 1; level >= 0; level-- {
		current := s.head
		fmt.Println("Level: ", level)

		for current != nil {

			fmt.Println(" --> ", current.record.GetKey())
			fmt.Println(" --> ", string(current.record.GetValue()))
			current = current.next
		}
		fmt.Println("nil")
		if s.head.below != nil {
			s.head = s.head.below
		}
	}
}

func (s *SkipList) Flush() {
	current := s.head
	for current.below != nil {
		current = current.below
	}

	// Preskoči head čvor
	current = current.next

	for current != nil {
		// Preskoči tail čvor (čvor sa najväčšim ključom)
		if current.record.GetKey() == "\xFF\xFF\xFF\xFF" {
			break
		}
		fmt.Printf("Key: %s, Value: %s, Timestamp: %d, Tombstone: %d\n",
			current.record.GetKey(), string(current.record.GetValue()), current.record.GetTimeStamp(), current.record.GetTombstone())
		current = current.next
	}
}
