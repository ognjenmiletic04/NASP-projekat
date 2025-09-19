package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
)

// Index predstavlja sparse index za Data segment.
type Index struct {
	fileName     string
	indexEntries []IndexEntry
}

// NewIndex kreira novi Index objekat.
func NewIndex(fileName string, entries []IndexEntry) *Index {
	return &Index{
		fileName:     fileName,
		indexEntries: entries,
	}
}

// GetFileName vraca ime fajla.
func (idx *Index) GetFileName() string {
	return idx.fileName
}

// SetFileName postavlja ime fajla.
func (idx *Index) SetFileName(name string) {
	idx.fileName = name
}

// GetIndexEntries vraca sve index zapise.
func (idx *Index) GetIndexEntries() []IndexEntry {
	return idx.indexEntries
}

// SetIndexEntries postavlja index zapise.
func (idx *Index) SetIndexEntries(entries []IndexEntry) {
	idx.indexEntries = entries
}

// WriteToFile snima index entries u fajl (blok po blok).
func (idx *Index) WriteToFile() error {
	if idx.fileName == "" {
		return fmt.Errorf("index file name is not set")
	}
	if len(idx.indexEntries) == 0 {
		return fmt.Errorf("no index entries to write")
	}

	// otvori fajl
	f, err := os.OpenFile(idx.fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("cannot open index file: %w", err)
	}
	defer f.Close()

	// upisi svaki zapis
	for _, entry := range idx.indexEntries {
		// 1) key size
		ks := uint64(len(entry.Key))
		ksBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(ksBytes, ks)
		if _, err := f.Write(ksBytes); err != nil {
			return err
		}

		// 2) key
		if _, err := f.Write(entry.Key); err != nil {
			return err
		}

		// 3) dataBlock
		dbBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(dbBytes, entry.DataBlock)
		if _, err := f.Write(dbBytes); err != nil {
			return err
		}

		// 4) offset
		offBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(offBytes, entry.Offset)
		if _, err := f.Write(offBytes); err != nil {
			return err
		}
	}

	return nil
}

// ReadFromFile ucitava sve IndexEntry iz fajla.
func (idx *Index) ReadFromFile() ([]IndexEntry, error) {
	f, err := os.Open(idx.fileName)
	if err != nil {
		return nil, fmt.Errorf("cannot open index file: %w", err)
	}
	defer f.Close()

	entries := make([]IndexEntry, 0)

	for {
		// 1) key size
		ksBytes := make([]byte, 8)
		_, err := f.Read(ksBytes)
		if err != nil {
			break // EOF
		}
		ks := binary.LittleEndian.Uint64(ksBytes)

		// 2) key
		key := make([]byte, ks)
		if _, err := f.Read(key); err != nil {
			return nil, err
		}

		// 3) dataBlock
		dbBytes := make([]byte, 4)
		if _, err := f.Read(dbBytes); err != nil {
			return nil, err
		}
		dataBlock := binary.LittleEndian.Uint32(dbBytes)

		// 4) offset
		offBytes := make([]byte, 4)
		if _, err := f.Read(offBytes); err != nil {
			return nil, err
		}
		offset := binary.LittleEndian.Uint32(offBytes)

		entries = append(entries, IndexEntry{
			Key:       key,
			DataBlock: dataBlock,
			Offset:    offset,
		})
	}

	return entries, nil
}

// BinarySearch pretrayuje entry-e po kljucu.
func (idx *Index) SearchIndex(entries []IndexEntry, key []byte) *IndexEntry {
	low, high := 0, len(entries)-1
	for low <= high {
		mid := (low + high) / 2
		cmp := string(entries[mid].Key)
		if cmp == string(key) {
			return &entries[mid]
		} else if cmp < string(key) {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	// ako nije tacan pogodak, vracamo "najblizi manji"
	if high >= 0 {
		return &entries[high]
	}
	return nil
}
