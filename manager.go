/*
Manager struktura za sad samo objedinjuje wal i block manager

Funkcija GET je vise za prikaz, u GET funkciji ne treba da se nalazi wal stavio sam da vidim da li mogu da dobavim neke rekord
PUT radi sta treba za sad i DELETE isto
*/
package main

import (
	"fmt"
	"project/blockmanager"
	wal "project/walFile"
)

type Manager struct {
	blockManager *blockmanager.BlockManager
	wal          *wal.WAL
}

func NewManager(blockSize uint64, poolSize uint64, blockNum uint64) *Manager {
	bufferPool := blockmanager.NewBufferPool()
	blockManager := blockmanager.NewBlockManager(bufferPool, blockSize, poolSize)
	wal := wal.NewWal(blockNum, blockManager)
	return &Manager{blockManager: blockManager, wal: wal}
}
func (manager *Manager) PUT(key string, value []byte) {
	record := blockmanager.SetRec(0, manager.wal.GetNumberOfRecords()+1, 0, uint64(len(key)), uint64(len(value)), key, value)
	manager.wal.WriteRecord(record, manager.blockManager)
	manager.blockManager.EmptyBufferPool() //samo za testiranje inace se prazni sam kad se popuni
	fmt.Println("Data written successfully")
}
func (manager *Manager) GET(key string) {
	manager.wal.ResetCounter()
	for {
		record := manager.wal.NextRecord(manager.blockManager)
		if record == nil {
			fmt.Println("There is no such record")
			return
		}
		if record.GetKey() == key {
			fmt.Println(record)
			return
		}
	}
}

func (manager *Manager) DELETE(key string) {
	value := make([]byte, 0)
	record := blockmanager.SetRec(0, manager.wal.GetNumberOfRecords()+1, 1, uint64(len(key)), uint64(len(value)), key, value)
	manager.wal.WriteRecord(record, manager.blockManager)
	manager.blockManager.EmptyBufferPool() //samo za testiranje inace se prazni sam kad se popuni
	fmt.Println("Data deleted successfully")
}
