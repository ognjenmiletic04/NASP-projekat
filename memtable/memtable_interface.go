package memtable

import "project/blockmanager"

// Globalne konstante za sve memtable tipove
// Ove konstante će u budućnosti biti učitane iz konfiguracionog fajla
const (
	DEFAULT_NUMBER_OF_TABLES   = 3
	DEFAULT_CAPACITY_PER_TABLE = 5
	DEFAULT_SKIP_LIST_HEIGHT   = 3
	DEFAULT_BTREE_MIN_DEGREE   = 3
)

// MemTableInterface definise operacije koje svaki memtable tip mora da implementira
type MemTableInterface interface {
	// PutRecord dodaje record u memtable, briše postojeću verziju sa istim kljucem
	PutRecord(record *blockmanager.Record)

	// Find pronalazi record po ključu, vraća nil ako ne postoji
	Find(key string) *blockmanager.Record

	// Flush ispisuje sadržaj memtable-a (za debug/SSTable kreiranje)
	Flush()

	// IsFull proverava da li je memtable popunjen i treba flush
	IsFull() bool

	// Clear briše sve podatke iz memtable-a
	Clear()

	// GetSize vraća broj trenutno čuvanih zapisa
	GetSize() int
}

// MemTableType enum za tipove memtable-a
type MemTableType int

const (
	TypeSkipList MemTableType = iota
	TypeHashMap
	TypeBTree
)

func (mt MemTableType) String() string {
	switch mt {
	case TypeSkipList:
		return "SkipList"
	case TypeHashMap:
		return "HashMap"
	case TypeBTree:
		return "BTree"
	default:
		return "Unknown"
	}
}

// CreateMemTable factory funkcija za kreiranje memtable-a
func CreateMemTable(memTableType MemTableType, capacity int) MemTableInterface {
	switch memTableType {
	case TypeSkipList:
		return NewSkipListMemTable(capacity, DEFAULT_NUMBER_OF_TABLES)
	case TypeHashMap:
		return NewHashMapMemTable(capacity, DEFAULT_NUMBER_OF_TABLES)
	case TypeBTree:
		return NewBTreeMemTable(capacity, DEFAULT_BTREE_MIN_DEGREE, DEFAULT_NUMBER_OF_TABLES)
	default:
		// Default fallback na SkipList
		return NewSkipListMemTable(capacity, DEFAULT_NUMBER_OF_TABLES)
	}
}
