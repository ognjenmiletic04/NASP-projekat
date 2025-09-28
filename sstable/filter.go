package sstable

import (
	"crypto/md5"
	"encoding/binary"
	"math"
	"os"
	"time"
)

// HashWithSeed je hash funkcija sa seed vrednoscu
type HashWithSeed struct {
	Seed []byte
}

func (h HashWithSeed) Hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

// Kreira k razlicitih hash funkcija sa razlicitim seed-ovima
func CreateHashFunctions(k uint32) []HashWithSeed {
	h := make([]HashWithSeed, k)
	ts := uint32(time.Now().Unix())
	for i := uint32(0); i < k; i++ {
		seed := make([]byte, 4)
		binary.BigEndian.PutUint32(seed, ts+i)
		hfn := HashWithSeed{Seed: seed}
		h[i] = hfn
	}
	return h
}

// Izracunava optimalnu velicinu bit niza (m)
func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) *
		math.Abs(math.Log(falsePositiveRate)) /
		math.Pow(math.Log(2), 2)))
}

// Izracunava optimalan broj hash funkcija (k)
func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

// BloomFilter struktura
type BloomFilter struct {
	HashFunctions []HashWithSeed
	BitArray      []bool
	m             uint // velicina bit niza
	k             uint // broj hash funkcija
}

// Konstruktor
func NewBloomFilter(expectedElements int, falsePositiveRate float64) *BloomFilter {
	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)
	return &BloomFilter{
		HashFunctions: CreateHashFunctions(uint32(k)),
		BitArray:      make([]bool, m),
		m:             m,
		k:             k,
	}
}

// Dodaje kljuc u Bloom filter
func (b *BloomFilter) Add(data []byte) {
	for i := 0; i < len(b.HashFunctions); i++ {
		vrednost := b.HashFunctions[i].Hash(data)
		indeks := vrednost % uint64(b.m)
		b.BitArray[indeks] = true
	}
}

// Proverava da li kljuc mozda postoji u filteru
func (b BloomFilter) Contains(data []byte) bool {
	for i := 0; i < len(b.HashFunctions); i++ {
		vrednost := b.HashFunctions[i].Hash(data)
		indeks := vrednost % uint64(b.m)
		if !b.BitArray[indeks] {
			return false
		}
	}
	return true
}

// Serializacija Bloom filtera u bajtove
func (b BloomFilter) WriteBloomFilterFile() []byte {
	bytes := make([]byte, 0)

	// upisi m i k
	mBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(mBytes, uint64(b.m))
	bytes = append(bytes, mBytes...)

	kBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(kBytes, uint64(b.k))
	bytes = append(bytes, kBytes...)

	// upisi bit array
	for i := 0; i < len(b.BitArray); i++ {
		if b.BitArray[i] {
			bytes = append(bytes, 1)
		} else {
			bytes = append(bytes, 0)
		}
	}

	// upisi seed-ove hash funkcija
	hashBytesLen := 4 * len(b.HashFunctions)
	hashBytes := make([]byte, hashBytesLen)
	for i := 0; i < len(b.HashFunctions); i++ {
		copy(hashBytes[i*4:], b.HashFunctions[i].Seed)
	}
	bytes = append(bytes, hashBytes...)

	return bytes
}

// Deserializacija iz fajla
func (b *BloomFilter) ReadBloomFilterFile(file *os.File) error {
	mBytes := make([]byte, 8)
	_, err := file.Read(mBytes)
	if err != nil {
		return err
	}
	b.m = uint(binary.LittleEndian.Uint64(mBytes))

	kBytes := make([]byte, 8)
	_, err = file.Read(kBytes)
	if err != nil {
		return err
	}
	b.k = uint(binary.LittleEndian.Uint64(kBytes))

	// citanje bit array-a
	b.BitArray = make([]bool, b.m)
	for i := 0; i < int(b.m); i++ {
		bit := make([]byte, 1)
		_, err = file.Read(bit)
		if err != nil {
			return err
		}
		if bit[0] == 0 {
			b.BitArray[i] = false
		} else {
			b.BitArray[i] = true
		}
	}

	// citanje seed-ova
	hashBytesLen := 4 * b.k
	hashBytes := make([]byte, hashBytesLen)
	_, err = file.Read(hashBytes)
	if err != nil {
		return err
	}
	b.HashFunctions = make([]HashWithSeed, b.k)
	for i := 0; i < int(b.k); i++ {
		seed := make([]byte, 4)
		copy(seed, hashBytes[i*4:(i+1)*4])
		hfn := HashWithSeed{Seed: seed}
		b.HashFunctions[i] = hfn
	}

	return nil
}
