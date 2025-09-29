package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
)

type SummaryEntry struct {
	Key         []byte
	IndexOffset int64
}

type Summary struct {
	fileName string
	entries  []SummaryEntry
}

// Getters/Setters
func (s *Summary) SetFileName(name string) { s.fileName = name }
func (s *Summary) GetFileName() string     { return s.fileName }

func (s *Summary) GetEntries() []SummaryEntry { return s.entries }

// WriteToFile – serijalizuje summary u fajl
func (s *Summary) WriteToFile() error {
	if s.fileName == "" {
		return fmt.Errorf("summary file name not set")
	}

	f, err := os.OpenFile(s.fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("cannot open summary file: %w", err)
	}
	defer f.Close()

	// helper za upis ključa
	writeKey := func(key []byte) error {
		ks := uint64(len(key))
		ksBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(ksBytes, ks)
		if _, err := f.Write(ksBytes); err != nil {
			return err
		}
		if _, err := f.Write(key); err != nil {
			return err
		}
		return nil
	}

	// upiši samo entries
	for _, e := range s.entries {
		if err := writeKey(e.Key); err != nil {
			return err
		}
		offBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(offBytes, uint64(e.IndexOffset))
		if _, err := f.Write(offBytes); err != nil {
			return err
		}
	}

	return nil
}

// ReadFromFile – deserijalizacija Summary fajla
func ReadFromFile(fileName string) ([]SummaryEntry, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("cannot open summary file: %w", err)
	}
	defer f.Close()

	readKey := func() ([]byte, error) {
		ksBytes := make([]byte, 8)
		_, err := f.Read(ksBytes)
		if err != nil {
			return nil, err
		}
		ks := binary.LittleEndian.Uint64(ksBytes)
		key := make([]byte, ks)
		if _, err := f.Read(key); err != nil {
			return nil, err
		}
		return key, nil
	}

	entries := make([]SummaryEntry, 0)
	for {
		key, err := readKey()
		if err != nil {
			break // EOF
		}

		offBytes := make([]byte, 8)
		if _, err := f.Read(offBytes); err != nil {
			return nil, err
		}
		offset := int64(binary.LittleEndian.Uint64(offBytes))

		entries = append(entries, SummaryEntry{
			Key:         key,
			IndexOffset: offset,
		})
	}

	return entries, nil
}
func NewSummary(filename string) *Summary {
	ent, _ := ReadFromFile(filename)

	return &Summary{
		fileName: filename,
		entries:  ent,
	}
}

func BuildSummaryFromIndex(indexFile string, summaryFile string, N int) (*Summary, error) {
	f, err := os.Open(indexFile)
	if err != nil {
		return nil, fmt.Errorf("cannot open index file: %w", err)
	}
	defer f.Close()

	summary := &Summary{fileName: summaryFile}
	entries := make([]SummaryEntry, 0)

	var offset int64 = 0
	var entryCount int = 0

	for {
		// 1) key size -- ovo make i 8 key je sigurno veci?
		ksBytes := make([]byte, 8)
		n, err := f.Read(ksBytes)
		if err != nil || n == 0 {
			break // EOF
		}
		if n < 8 {
			return nil, fmt.Errorf("corrupted index file")
		}
		ks := binary.LittleEndian.Uint64(ksBytes)
		offset += int64(n)

		// 2) key
		key := make([]byte, ks)
		n, err = f.Read(key)
		if err != nil || n == 0 {
			return nil, fmt.Errorf("corrupted index file")
		}
		offset += int64(n)

		// 3) offset (4 bajta)
		offBytes := make([]byte, 4)
		n, err = f.Read(offBytes)
		if err != nil {
			return nil, fmt.Errorf("corrupted index file")
		}
		offset += int64(n)

		if entryCount%N == 0 {
			entries = append(entries, SummaryEntry{
				Key:         append([]byte(nil), key...),
				IndexOffset: offset - (8 + int64(len(key)) + 4),
			})
		}

		entryCount++
	}

	summary.entries = entries
	return summary, nil
}

func (s *Summary) Find(target []byte) (int64, bool) {
	if len(s.entries) == 0 {
		return 0, false
	}

	lo, hi := 0, len(s.entries)-1
	var candidate int64 = -1

	for lo <= hi {
		mid := (lo + hi) / 2
		if string(s.entries[mid].Key) >= string(target) {
			candidate = s.entries[mid].IndexOffset
			hi = mid - 1
		} else {
			lo = mid + 1
		}
	}

	if candidate == -1 {
		// target veći od svih → vrati poslednji offset
		return s.entries[len(s.entries)-1].IndexOffset, true
	}
	return candidate, true
}
