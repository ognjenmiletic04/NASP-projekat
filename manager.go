/*
Manager struktura za sad samo objedinjuje wal i block manager

Funkcija GET je vise za prikaz, u GET funkciji ne treba da se nalazi wal stavio sam da vidim da li mogu da dobavim neke rekord
PUT radi sta treba za sad i DELETE isto
*/
package main

import (
	"fmt"
	"project/blockmanager"
	"project/memtable"
	wal "project/walFile"
)

type Manager struct {
	blockManager *blockmanager.BlockManager
	wal          *wal.WAL
	memtable     memtable.MemTableInterface // Koristi interface umesto konkretni tip
}

func NewManager(blockSize uint64, poolSize uint64, blockNum uint64, memTableType memtable.MemTableType) *Manager {
	bufferPool := blockmanager.NewBufferPool()
	blockManager := blockmanager.NewBlockManager(bufferPool, blockSize, poolSize)
	wal := wal.NewWal(blockNum, blockManager)

	// Kreiraj memtable sa izabranim tipom
	capacity := 5 // default capacity
	mt := memtable.CreateMemTable(memTableType, capacity)
	loadFromWAL(mt, wal)

	return &Manager{
		blockManager: blockManager,
		wal:          wal,
		memtable:     mt,
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
		record := wal.NextRecord(wal.GetBlockManager())
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
		return record.GetValue()
	}

	// Drugo: Fallback na WAL pretragu (sporije)
	fmt.Println("Not found in memtable, searching in WAL...")
	manager.wal.ResetCounter()

	var latestRecord *blockmanager.Record

	// Pronađi poslednju verziju ključa u WAL-u
	for {
		record := manager.wal.NextRecord(manager.blockManager)
		if record == nil {
			break
		}
		if record.GetKey() == key {
			if latestRecord == nil || record.GetTimeStamp() > latestRecord.GetTimeStamp() {
				latestRecord = record
			}
		}
	}

	if latestRecord == nil {
		fmt.Printf("Key '%s' not found\n", key)
		return nil
	}

	if latestRecord.GetTombstone() == 1 {
		fmt.Printf("Key '%s' is deleted (tombstone found in WAL)\n", key)
		return nil
	}

	fmt.Printf("Found in WAL: %s = %s\n", key, string(latestRecord.GetValue()))
	return latestRecord.GetValue()
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
