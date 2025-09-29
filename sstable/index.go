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

// WriteToFile snima index entries u fajl.
func (idx *Index) WriteToFile() error {
	if idx.fileName == "" {
		return fmt.Errorf("index file name is not set")
	}
	if len(idx.indexEntries) == 0 {
		return fmt.Errorf("no index entries to write")
	}

	f, err := os.OpenFile(idx.fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("cannot open index file: %w", err)
	}
	defer f.Close()

	for _, entry := range idx.indexEntries {
		// 1) key size
		ks := uint64(len(entry.Key))
		ksBytes := make([]byte, 8) //key je sigurno veci
		binary.LittleEndian.PutUint64(ksBytes, ks)
		if _, err := f.Write(ksBytes); err != nil {
			return err
		}

		// 2) key
		if _, err := f.Write(entry.Key); err != nil {
			return err
		}

		// 3) offset (blok broj)
		offBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(offBytes, entry.Offset)
		if _, err := f.Write(offBytes); err != nil {
			return err
		}
	}

	return nil
}

// ReadFromFile učitava sve IndexEntry iz fajla.
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

		// 3) offset (blok broj)
		offBytes := make([]byte, 4)
		if _, err := f.Read(offBytes); err != nil {
			return nil, err
		}
		offset := binary.LittleEndian.Uint32(offBytes)

		entries = append(entries, IndexEntry{
			Key:    key,
			Offset: offset,
		})
	}
	idx.indexEntries = entries
	return entries, nil
}

// SearchIndex – binarna pretraga kroz indexEntries.
// Vraća candidate offset (broj bloka) i bool found.
func (idx *Index) SearchIndex(target []byte) (uint32, bool) {
	if len(idx.indexEntries) == 0 {
		return 0, false
	}

	lo, hi := 0, len(idx.indexEntries)-1
	var candidate uint32 = 0
	found := false

	for lo <= hi {
		mid := (lo + hi) / 2
		cmp := string(idx.indexEntries[mid].Key)

		if cmp == string(target) {
			// tačan pogodak
			return idx.indexEntries[mid].Offset, true
		} else if cmp > string(target) {
			candidate = idx.indexEntries[mid].Offset
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}

	// ako nismo pogodili, candidate će biti offset prvog ključa > target
	// ako je target veći od svih, uzimamo poslednji offset
	if !found {
		if candidate == 0 {
			candidate = idx.indexEntries[len(idx.indexEntries)-1].Offset
		}
	}

	return candidate, false
}
