package memtable

import (
	"fmt"
	"project/blockmanager"
)

var (
	SKIP_LIST_CAPACITY int = 5
	NUMBER_OF_TABLES   int = 3
	SKIP_LIST_HEIGHT   int = 3
)

type MemTable struct {
	tables []*SkipList
}

func NewMemTable() *MemTable {
	tables := make([]*SkipList, NUMBER_OF_TABLES)
	for i := 0; i < NUMBER_OF_TABLES; i++ {
		tables[i] = NewSkipList(SKIP_LIST_HEIGHT)
	}
	return &MemTable{tables: tables}
}

func (memtable *MemTable) PutNode(record *blockmanager.Record) {
	for i := 0; i < NUMBER_OF_TABLES; i++ {
		currentSkipList := memtable.tables[i]
		if currentSkipList.currentCapacity < SKIP_LIST_CAPACITY {
			currentSkipList.Insert(record)
			break
		}
	}

	if memtable.tables[NUMBER_OF_TABLES-1].currentCapacity == SKIP_LIST_CAPACITY {
		memtable.Flush()
		for i := 0; i < NUMBER_OF_TABLES; i++ {
			memtable.tables[i] = NewSkipList(SKIP_LIST_HEIGHT)
		}
	}

}

func (memtable *MemTable) Flush() {
	for i := 0; i < NUMBER_OF_TABLES; i++ {
		currentSkipList := memtable.tables[i]
		fmt.Println("Memtable", i)
		//Treba samo izmeniti flush funkciju da upisuje u fajl
		currentSkipList.Flush()
	}
}

// Find metoda za pronalaženje record-a po ključu
func (memtable *MemTable) Find(key string) *blockmanager.Record {
	// Prolazi kroz sve SkipList tabele i traži ključ
	for i := 0; i < NUMBER_OF_TABLES; i++ {
		currentSkipList := memtable.tables[i]
		node := currentSkipList.Find(key)
		if node != nil {
			return node.record // Vraćamo record iz Node-a
		}
	}
	return nil // Record nije pronađen
}
