package memtable

import (
	"fmt"
	"project/blockmanager"
)

// BTreeMemTable implementira memtable koristeći N B-tree tabela
type BTreeMemTable struct {
	btrees     []*BTree
	capacity   int
	numTables  int
	sizes      []int
	childCount int
}

// BTreeMemTable implementira MemTableInterface
var _ MemTableInterface = (*BTreeMemTable)(nil)

// NewBTreeMemTable kreira novi BTree-based memtable sa N tabela
func NewBTreeMemTable(capacity int, minDegree int, numTables int) *BTreeMemTable {
	btrees := make([]*BTree, numTables)
	sizes := make([]int, numTables)
	for i := 0; i < numTables; i++ {
		btrees[i] = NewBTree(minDegree)
		sizes[i] = 0
	}
	return &BTreeMemTable{
		btrees:     btrees,
		capacity:   capacity,
		numTables:  numTables,
		sizes:      sizes,
		childCount: minDegree,
	}
}

func (bmt *BTreeMemTable) PutRecord(record *blockmanager.Record) {
	key := record.GetKey()

	// Pretraži sve tabele da vidiš da li ključ već postoji
	for i := 0; i < bmt.numTables; i++ {
		existingRecord := bmt.btrees[i].Search(key)
		if existingRecord != nil {
			//Zamena recorda ukoliko vec postoji
			bmt.btrees[i].ReplaceRecord(key, record)
			return
		}
	}

	// Ključ ne postoji - dodaj novi record u aktivnu tabelu
	activeTableIndex := -1
	for i := 0; i < bmt.numTables; i++ {
		if bmt.sizes[i] < bmt.capacity {
			activeTableIndex = i
			break
		}
	}

	// Ako nema aktivne tabele (sve su pune), flush i restartuj
	if activeTableIndex == -1 {
		bmt.Flush()
		bmt.Clear()
		activeTableIndex = 0 // Nakon clear-a, prva tabela postaje aktivna
	}

	bmt.btrees[activeTableIndex].Insert(record)
	bmt.sizes[activeTableIndex]++
}

// Find pronalazi record po ključu u svim B-tree tabelama
func (bmt *BTreeMemTable) Find(key string) *blockmanager.Record {
	for i := 0; i < bmt.numTables; i++ {
		if record := bmt.btrees[i].Search(key); record != nil {
			return record
		}
	}
	return nil
}

// Nakon dodavanja SS table, Flush ce zapravo biti poziv za praznjenje memtablea kad je pun
// Flush ispisuje sadržaj svih B-tree tabela
func (bmt *BTreeMemTable) Flush() {
	fmt.Println("=== BTree MemTable ===")

	// Pronađi aktivnu tabelu (prva nepuna)
	activeTableIndex := -1
	for i := 0; i < bmt.numTables; i++ {
		if bmt.sizes[i] < bmt.capacity {
			activeTableIndex = i
			break
		}
	}

	for i := 0; i < bmt.numTables; i++ {
		status := "READ-ONLY"
		if i == activeTableIndex {
			status = "READ-WRITE (Active)"
		} else if activeTableIndex == -1 {
			status = "FULL"
		}

		fmt.Printf("BTree Table %d (%s): %d/%d\n", i, status, bmt.sizes[i], bmt.capacity)

		if bmt.sizes[i] > 0 {
			// Pribavi sve rekorde iz B-tree i prikaži ih detaljno
			records := bmt.btrees[i].GetAllRecords()
			for _, record := range records {
				tombstone := "0"
				if record.GetTombstone() == 1 {
					tombstone = "1"
				}
				value := string(record.GetValue())
				if record.GetTombstone() == 1 {
					value = ""
				}
				fmt.Printf("Key: %s, Value: %s, Timestamp: %d, Tombstone: %s\n",
					record.GetKey(), value, record.GetTimeStamp(), tombstone)
			}
		}
	}
}

// IsFull proverava da li su SVE B-tree tabele pune (potreban flush)
func (bmt *BTreeMemTable) IsFull() bool {
	for i := 0; i < bmt.numTables; i++ {
		if bmt.sizes[i] < bmt.capacity {
			return false
		}
	}
	return true
}

// Clear briše sve podatke iz svih B-tree tabela
func (bmt *BTreeMemTable) Clear() {
	for i := 0; i < bmt.numTables; i++ {
		bmt.btrees[i] = NewBTree(bmt.childCount)
		bmt.sizes[i] = 0
	}
}

// GetSize vraća ukupan broj trenutno čuvanih zapisa
func (bmt *BTreeMemTable) GetSize() int {
	total := 0
	for i := 0; i < bmt.numTables; i++ {
		total += bmt.sizes[i]
	}
	return total
}
