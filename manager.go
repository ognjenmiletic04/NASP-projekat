/*
Manager struktura za sad samo objedinjuje wal i block manager

Funkcija GET je vise za prikaz, u GET funkciji ne treba da se nalazi wal stavio sam da vidim da li mogu da dobavim neke rekord
PUT radi sta treba za sad i DELETE isto
*/
package main

import (
	"fmt"
	"os"
	"project/blockmanager"
	"project/cache"
	"project/memtable"
	"project/sstable"
	wal "project/walFile"
)

type Manager struct {
	blockManager *blockmanager.BlockManager
	wal          *wal.WAL
	memtable     memtable.MemTableInterface // Koristi interface umesto konkretni tip
	cache        *cache.Cache
	data         *sstable.Data // dodaj referencu na Data
	index        *sstable.Index
	summary      *sstable.Summary
	filter       *sstable.BloomFilter
	mtree        *sstable.MerkleTree
}

var conf *Config

func init() {
	var err error
	conf, err = LoadConfig("config.json")
	if err != nil {
		panic(fmt.Sprintf("greska pri ucitavanju configa: %v", err))
	}
}

//funckija za imena file da ne ide sve uvek u 1.

func NewManager(memTableType memtable.MemTableType) *Manager {

	bufferPool := blockmanager.NewBufferPool()
	blockManager := blockmanager.NewBlockManager(bufferPool, conf.BlockSize, conf.PoolSize)
	wal := wal.NewWal(conf.BlockNum, blockManager)

	// Kreiraj memtable sa izabranim tipom
	mt := memtable.CreateMemTable(memTableType, conf.MemCapacity)
	loadFromWAL(mt, wal)

	ch := cache.NewCache(conf.CacheCapacity)

	dt := sstable.NewData("sstable/DATA/usertable-00001-Data.db", conf.BlockSize, conf.PoolSize)
	idx := sstable.NewIndex("sstable/INDEX/usertable-00001-Index.db", nil) // za početak prazan
	expectedElements := memtable.DEFAULT_CAPACITY_PER_TABLE * memtable.DEFAULT_NUMBER_OF_TABLES
	bf := sstable.NewBloomFilter(expectedElements, 0.01)
	s := sstable.NewSummary("sstable/SUMMARY/usertable-00001-Summary.db")
	return &Manager{
		blockManager: blockManager,
		wal:          wal,
		memtable:     mt,
		cache:        ch,
		data:         dt,
		index:        idx,
		summary:      s,
		filter:       bf,
		mtree:        nil,
	}
}

// Funkcija za učitavanje memtable iz WAL-a pri startup-u - optimizovana verzija
func loadFromWAL(mt memtable.MemTableInterface, wal *wal.WAL) {
	fmt.Println("Loading memtable from WAL...")

	// Reset counter da čita od početka
	wal.ResetCounter()

	// Mapa za čuvanje poslednje verzije svakog ključa
	keyMap := make(map[string]*blockmanager.Record)

	totalRecords := 0
	for {
		record, hasNext := wal.NextRecord(wal.GetBlockManager())
		if record == nil {
			break // Nema više zapisa
		}
		totalRecords++
		key := record.GetKey()

		// Uvek uzmi poslednju verziju ključa (newer timestamp/sequence wins)
		existingRecord, exists := keyMap[key]
		if !exists || record.GetTimeStamp() > existingRecord.GetTimeStamp() {
			keyMap[key] = record
		}
		if !hasNext {
			break // Nema više zapisa
		}
	}

	uniqueRecords := 0
	for _, record := range keyMap {
		mt.PutRecord(record)
		uniqueRecords++
	}

	fmt.Printf("Loaded %d total records from WAL, %d unique keys into memtable\n", totalRecords, uniqueRecords)
}

func (manager *Manager) PUT(key string, value []byte) error {
	record := blockmanager.SetRec(0, manager.wal.GetNumberOfRecords()+1, 0, uint64(len(key)), uint64(len(value)), key, value)

	//Pokušaj upis u WAL i provera uspešnost
	err := manager.wal.WriteRecord(record, manager.blockManager)
	if err != nil {
		return fmt.Errorf("failed to write to WAL: %v", err)
	}

	// Nakon uspešnog WAL zapisa: Dodaj u memtable
	manager.memtable.PutRecord(record)
	manager.filter.Add([]byte(key))

	// Ako je memtable pun → flush u Data fajl
	if manager.memtable.IsFull() {
		records, err := manager.memtable.Flush()
		if err == nil && len(records) > 0 {
			indexEntries, err := manager.data.WriteDataFile(records)
			if err != nil {
				return fmt.Errorf("failed to flush memtable to SSTable: %v", err)
			}
			manager.index.SetIndexEntries(indexEntries)
			if err := manager.index.WriteToFile(); err != nil {
				return fmt.Errorf("\nfailed to write index: %v", err)
			}

			// ovde napraviti i summary
			summaryFile := "sstable/SUMMARY/usertable-00001-Summary.db"
			smr, err := sstable.BuildSummaryFromIndex(
				manager.index.GetFileName(), // uzmi index fajl koji si upravo napravio
				summaryFile,                 // gde da snimi summary
				conf.SummaryStep,            // N = svaki 5. entry ide u summary (podesi po želji)
			)
			if err != nil {
				return fmt.Errorf("failed to build summary: %v", err)
			}
			manager.summary = smr
			if err := manager.summary.WriteToFile(); err != nil {
				return fmt.Errorf("failed to write summary: %v", err)
			}

			//upis bloomfiltera
			bfFile, err := os.Create("sstable/FILTER/usertable-00001-Filter.db")
			if err != nil {
				return fmt.Errorf("failed to create bloom filter file: %v", err)
			}
			defer bfFile.Close()

			_, err = bfFile.Write(manager.filter.WriteBloomFilterFile())
			if err != nil {
				return fmt.Errorf("failed to write bloom filter: %v", err)
			}

			manager.mtree = sstable.CreateMerkleTree(manager.data.GetDataBlocks(memtable.DEFAULT_NUMBER_OF_TABLES, manager.data.GetFileName()))
			manager.mtree.Serialize("sstable/METADATA/usertable-00001-Metadata.db")

			fmt.Println("MemTable flushed to SSTable")
		}
	}

	manager.blockManager.EmptyBufferPool() //samo za testiranje inace se prazni sam kad se popuni
	fmt.Println("Data written successfully")
	return nil
}

func (manager *Manager) GET(key string) []byte {
	fmt.Printf("Searching for key: %s\n", key)

	// Prvo Traži u memtable (najbrže)
	record := manager.memtable.Find(key)
	if record != nil {
		if record.GetTombstone() == 1 {
			fmt.Printf("Key '%s' is deleted (tombstone found in memtable)\n", key)
			return nil
		}
		fmt.Printf("Found in memtable: %s = %s\n", key, string(record.GetValue()))
		manager.cache.Put(record)
		return record.GetValue()
	}

	// Drugo: Trazi u cache
	record, ok := manager.cache.Get(key)
	if ok {
		if record.GetTombstone() == 1 {
			fmt.Printf("Key '%s' is deleted (tombstone found in cache)\n", key)
			return nil
		}
		fmt.Printf("Found in cache: %s = %s\n", key, string(record.GetValue()))
		return record.GetValue()
	}

	//Trece: Trazi u BloomFilter
	file, _ := os.Open("sstable/FILTER/usertable-00001-Filter.db")
	manager.filter.ReadBloomFilterFile(file)
	if manager.filter.Contains([]byte(key)) {
		//Ako je mozda u BF, idemo dalje

		// Četvrto - summary sstabla-a
		/*entries, err := sstable.ReadFromFile("sstable/SUMMARY/usertable-00001-Summary.db")
		if err != nil {
			fmt.Printf("Greska pri citanju summary fajla: %v\n", err)
			return nil
		}*/

		_, found := manager.summary.Find([]byte(key))
		if !found {
			// Nije u summary → ne postoji
			return nil
		} else {
			//Mozda ga ima u summary udji u indeks
			_, err := manager.index.ReadFromFile()
			if err != nil {
				fmt.Printf("Greska pri citanju summary fajla: %v\n", err)
				return nil
			} else {
				//Trazi u index

				indexCandidateOffset, found1 := manager.index.SearchIndex([]byte(key))
				if !found1 {
					return nil
				} else {
					//ako ga mozda ima u index, trazi u data block iz sstable data
					record, found2, err := manager.data.FindInBlock(indexCandidateOffset, []byte(key))
					if err != nil {
						fmt.Printf("Greska pri citanju summary fajla: %v\n", err)
						return nil
					} else {
						if !found2 {
							return nil
						} else {
							manager.cache.Put(record)
							fmt.Printf("Found in sstable")
							return record.GetValue()
						}
					}
				}
			}
		}

	} else {
		//Sigurno se ne nalazi u BloomFilteru
		return nil
	}

}

func (manager *Manager) DELETE(key string) error {
	value := make([]byte, 0)
	record := blockmanager.SetRec(0, manager.wal.GetNumberOfRecords()+1, 1, uint64(len(key)), uint64(len(value)), key, value)

	// PRVO: Pokušaj upis delete marker-a u WAL i proveri uspešnost
	err := manager.wal.WriteRecord(record, manager.blockManager)
	if err != nil {
		return fmt.Errorf("failed to write delete marker to WAL: %v", err)
	}

	// TEK NAKON uspešnog WAL zapisa: Dodaj delete marker u memtable
	manager.memtable.PutRecord(record)

	manager.blockManager.EmptyBufferPool() //samo za testiranje inace se prazni sam kad se popuni
	fmt.Println("Data deleted successfully")
	return nil
}
