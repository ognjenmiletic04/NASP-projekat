package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
)

// SummaryEntry – predstavlja sample iz Index fajla
type SummaryEntry struct {
	Key         []byte
	IndexOffset int64 // offset u index fajlu gde ovaj zapis pocinje
}

// Summary – struktura Summary fajla
type Summary struct {
	fileName string
	minKey   []byte
	maxKey   []byte
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

	// upisi minKey i maxKey
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

	if err := writeKey(s.minKey); err != nil {
		return err
	}
	if err := writeKey(s.maxKey); err != nil {
		return err
	}

	// upisi sample-ovane zapise
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
func (s *Summary) ReadFromFile() ([]SummaryEntry, error) {
	f, err := os.Open(s.fileName)
	if err != nil {
		return nil, fmt.Errorf("cannot open summary file: %w", err)
	}
	defer f.Close()

	// prvo minKey i maxKey
	readKey := func() ([]byte, error) {
		ksBytes := make([]byte, 8)
		if _, err := f.Read(ksBytes); err != nil {
			return nil, err
		}
		ks := binary.LittleEndian.Uint64(ksBytes)
		key := make([]byte, ks)
		if _, err := f.Read(key); err != nil {
			return nil, err
		}
		return key, nil
	}

	s.minKey, err = readKey()
	if err != nil {
		return nil, err
	}
	s.maxKey, err = readKey()
	if err != nil {
		return nil, err
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

	s.entries = entries
	return entries, nil
}

// BuildSummaryFromIndex kreira Summary iz Index fajla.
// N je proredjenost summary (npr. 5 znaci svaki 5. Index zapis). -->1.3[DZ1]
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
		// 1) key size
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

		// 3) dataBlock
		dbBytes := make([]byte, 4)
		n, err = f.Read(dbBytes)
		if err != nil {
			return nil, fmt.Errorf("corrupted index file")
		}
		offset += int64(n)

		// 4) offset
		offBytes := make([]byte, 4)
		n, err = f.Read(offBytes)
		if err != nil {
			return nil, fmt.Errorf("corrupted index file")
		}
		offset += int64(n)

		// prvi key = minKey
		if entryCount == 0 {
			summary.minKey = append([]byte(nil), key...)
		}
		// uvek zadnji procitan = maxKey
		summary.maxKey = append([]byte(nil), key...)

		// svaki N-ti zapis dodaj u summary
		if entryCount%N == 0 {
			entries = append(entries, SummaryEntry{
				Key:         append([]byte(nil), key...),
				IndexOffset: offset - (8 + int64(len(key)) + 4 + 4),
			})
		}

		entryCount++
	}

	summary.entries = entries
	return summary, nil
}

// FindRange koristi Summary da pronadje raspon offseta u Index fajlu gde moye biti kljuc.
func (s *Summary) FindRange(target []byte) (int64, int64, bool) {
	// van granica
	if string(target) < string(s.minKey) || string(target) > string(s.maxKey) {
		return 0, 0, false
	}

	// binarna pretraga po sample entries
	lo, hi := 0, len(s.entries)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		if string(s.entries[mid].Key) <= string(target) {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	// hi je indeks poslednjeg sample <= target
	start := s.entries[hi].IndexOffset
	var end int64
	if hi+1 < len(s.entries) {
		end = s.entries[hi+1].IndexOffset
	} else {
		end = -1 // do kraja fajla
	}

	return start, end, true
}
