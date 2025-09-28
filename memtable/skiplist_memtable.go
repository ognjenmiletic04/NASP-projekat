package memtable

import (
	"fmt"
	"project/blockmanager"
)

type SkipListMemTable struct {
	tables    []*SkipList
	capacity  int
	numTables int
}

var _ MemTableInterface = (*SkipListMemTable)(nil)

func NewSkipListMemTable(capacity int, numTables int) *SkipListMemTable {
	tables := make([]*SkipList, numTables)
	for i := 0; i < numTables; i++ {
		tables[i] = NewSkipList(DEFAULT_SKIP_LIST_HEIGHT)
	}
	return &SkipListMemTable{
		tables:    tables,
		numTables: numTables,
		capacity:  capacity,
	}
}

// PutRecord dodaje record u SkipList tabele
func (smt *SkipListMemTable) PutRecord(record *blockmanager.Record) {
	key := record.GetKey()

	// Uklanjanje duplikata ako postoje
	for i := 0; i < smt.numTables; i++ {
		if smt.tables[i].Delete(key) {
			break
		}
	}

	activeTableIndex := -1
	for i := 0; i < smt.numTables; i++ {
		if smt.tables[i].currentCapacity < smt.capacity {
			activeTableIndex = i
			break
		}
	}

	// Ako nema aktivne tabele (sve su pune), flush i restartuj
	if activeTableIndex == -1 {
		smt.Flush()
		smt.Clear()
		activeTableIndex = 0
	}

	smt.tables[activeTableIndex].Insert(record)
}

// Find pronalazi record po ključu kroz SkipList tabele
func (smt *SkipListMemTable) Find(key string) *blockmanager.Record {
	for i := 0; i < smt.numTables; i++ {
		currentSkipList := smt.tables[i]
		node := currentSkipList.Find(key)
		if node != nil {
			return node.record
		}
	}
	return nil
}

func (smt *SkipListMemTable) Dump() {
	fmt.Println("=== SkipList MemTable ===")

	// Pronađi aktivnu tabelu
	activeTableIndex := -1
	for i := 0; i < smt.numTables; i++ {
		if smt.tables[i].currentCapacity < smt.capacity {
			activeTableIndex = i
			break
		}
	}

	for i := 0; i < smt.numTables; i++ {
		currentSkipList := smt.tables[i]
		status := "READ-ONLY"
		if i == activeTableIndex {
			status = "READ-WRITE (Active)"
		} else if activeTableIndex == -1 {
			status = "FULL"
		}
		fmt.Printf("SkipList Table %d (%s): %d/%d\n", i, status,
			currentSkipList.currentCapacity, smt.capacity)
		currentSkipList.Flush()
	}
}

// Flush funkcija prazni memtable u SStable
// Prva nepuna tabela je READ-WRITE (aktivna), ostale su READ-ONLY
func (smt *SkipListMemTable) Flush() ([]*blockmanager.Record, error) {
	var outputs []*blockmanager.Record

	for i := 0; i < smt.numTables; i++ {
		currentSkipList := smt.tables[i]

		// Idi do dna (level 0)
		node := currentSkipList.head
		for node.below != nil {
			node = node.below
		}

		// Preskoči head sentinel
		node = node.next

		// Dodaj sve slogove do tail sentinela
		for node != nil && node.record != nil {
			outputs = append(outputs, node.record)
			node = node.next
		}
	}

	// Pošto skip lista već održava redosled po ključu,
	smt.Clear()
	return outputs, nil
}

// IsFull proverava da li su SVE tabele pune (potreban flush)
func (smt *SkipListMemTable) IsFull() bool {
	for i := 0; i < smt.numTables; i++ {
		if smt.tables[i].currentCapacity < smt.capacity {
			return false
		}
	}
	return true
}

// Clear briše sve podatke iz SkipList tabela
func (smt *SkipListMemTable) Clear() {
	for i := 0; i < smt.numTables; i++ {
		smt.tables[i] = NewSkipList(DEFAULT_SKIP_LIST_HEIGHT)
	}
}

// GetSize vraća ukupan broj zapisa u svim tabelama
func (smt *SkipListMemTable) GetSize() int {
	totalSize := 0
	for i := 0; i < smt.numTables; i++ {
		totalSize += smt.tables[i].currentCapacity
	}
	return totalSize
}
