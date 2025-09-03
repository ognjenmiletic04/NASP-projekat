package memtable

import (
	"fmt"
	"project/blockmanager"
)

type HashMapMemTable struct {
	tables    []map[string]*blockmanager.Record
	capacity  int
	numTables int
}

// HashMapMemTable implementira MemTableInterface
var _ MemTableInterface = (*HashMapMemTable)(nil)

func NewHashMapMemTable(capacity int, numTables int) *HashMapMemTable {
	tables := make([]map[string]*blockmanager.Record, numTables)
	for i := 0; i < numTables; i++ {
		tables[i] = make(map[string]*blockmanager.Record)
	}
	return &HashMapMemTable{
		tables:    tables,
		capacity:  capacity,
		numTables: numTables,
	}
}

// PutRecord dodaje record u hash mape
func (hmt *HashMapMemTable) PutRecord(record *blockmanager.Record) {
	key := record.GetKey()

	// Uklanjanje duplikata ako postoji
	for i := 0; i < hmt.numTables; i++ {
		if _, exists := hmt.tables[i][key]; exists {
			delete(hmt.tables[i], key)
			break
		}
	}

	// Pronalazenje aktivne tabele
	activeTableIndex := -1
	for i := 0; i < hmt.numTables; i++ {
		if len(hmt.tables[i]) < hmt.capacity {
			activeTableIndex = i
			break
		}
	}

	// Ako nema aktivne tabele (sve su pune), flush i restartuj
	if activeTableIndex == -1 {
		hmt.Flush()
		hmt.Clear()
		activeTableIndex = 0
	}

	hmt.tables[activeTableIndex][key] = record
}

// Find pronalazi record po ključu u svim tabelama
func (hmt *HashMapMemTable) Find(key string) *blockmanager.Record {

	for i := 0; i < hmt.numTables; i++ {
		if record, exists := hmt.tables[i][key]; exists {
			return record
		}
	}
	return nil
}

// Flush ce sluziti za praznjenje memtablea u SStable kad on bude implementiran
// Flush ispisuje sadržaj svih hash mapa
func (hmt *HashMapMemTable) Flush() {
	fmt.Println("=== HashMap MemTable ===")

	activeTableIndex := -1
	for i := 0; i < hmt.numTables; i++ {
		if len(hmt.tables[i]) < hmt.capacity {
			activeTableIndex = i
			break
		}
	}

	for tableIdx := 0; tableIdx < hmt.numTables; tableIdx++ {
		status := "READ-ONLY"
		if tableIdx == activeTableIndex {
			status = "READ-WRITE (Active)"
		} else if activeTableIndex == -1 {
			status = "FULL"
		}

		fmt.Printf("HashMap Table %d (%s): %d/%d\n", tableIdx, status,
			len(hmt.tables[tableIdx]), hmt.capacity)

		for key, record := range hmt.tables[tableIdx] {
			tombstone := "0"
			if record.GetTombstone() == 1 {
				tombstone = "1"
			}
			value := string(record.GetValue())
			if record.GetTombstone() == 1 {
				value = ""
			}
			fmt.Printf("Key: %s, Value: %s, Timestamp: %d, Tombstone: %s\n",
				key, value, record.GetTimeStamp(), tombstone)
		}
	}
} // IsFull proverava da li su SVE hash mape pune (potreban flush)
func (hmt *HashMapMemTable) IsFull() bool {
	for i := 0; i < hmt.numTables; i++ {
		if len(hmt.tables[i]) < hmt.capacity {
			return false
		}
	}
	return true
}

// Clear briše sve podatke iz svih hash mapa
func (hmt *HashMapMemTable) Clear() {
	for i := 0; i < hmt.numTables; i++ {
		hmt.tables[i] = make(map[string]*blockmanager.Record)
	}
}

// GetSize vraća ukupan broj trenutno čuvanih zapisa
func (hmt *HashMapMemTable) GetSize() int {
	total := 0
	for i := 0; i < hmt.numTables; i++ {
		total += len(hmt.tables[i])
	}
	return total
}
