package sstable

import (
	"fmt"
	"os"
	"project/blockmanager"
)

// IndexEntry predstavlja sparse index entry (prvi key u data bloku)
type IndexEntry struct {
	Key       []byte
	DataBlock uint32 // broj bloka u data fajlu (blok 0 = header)
	Offset    uint32 // offset unutar bloka (ovde koristimo 0 za pocetak bloka)
}

// Data predstavlja glavni segment SSTable fajla
type Data struct {
	fileName     string
	blockSize    uint64
	blockManager *blockmanager.BlockManager
	numRecords   uint64
}

// Konstruktor
func NewData(fileName string, blockSize uint64, poolSize uint64) *Data {
	return &Data{
		fileName:  fileName,
		blockSize: blockSize,
		blockManager: blockmanager.NewBlockManager(
			blockmanager.NewBufferPool(),
			blockSize,
			poolSize,
		),
		numRecords: 0,
	}
}

// Getteri i setteri
func (d *Data) GetFileName() string {
	return d.fileName
}
func (d *Data) SetFileName(name string) {
	d.fileName = name
}

func (d *Data) GetBlockSize() uint64 {
	return d.blockSize
}
func (d *Data) SetBlockSize(size uint64) {
	d.blockSize = size
}

func (d *Data) GetBlockManager() *blockmanager.BlockManager {
	return d.blockManager
}
func (d *Data) SetBlockManager(bm *blockmanager.BlockManager) {
	d.blockManager = bm
}

func (d *Data) GetNumRecords() uint64 {
	return d.numRecords
}
func (d *Data) SetNumRecords(n uint64) {
	d.numRecords = n
}

// WriteDataFile upisuje sve rekorde iz memtable u .data fajl koristeći BlockManager.
// Na kraj fajla dopisuje i Index blok.
func (d *Data) WriteDataFile(records []*blockmanager.Record) (indexEntries []IndexEntry, err error) {

	os.Create(d.fileName)

	// upiši header
	blockmanager.WriteHeader(d.fileName, d.blockSize)

	if len(records) == 0 {
		return nil, nil
	}

	var (
		currentBlockNum uint32 = 1
		curRecords             = make([]*blockmanager.Record, 0, 64)
		curBlockBytes   uint64 = 0
		firstKeyInBlock []byte
	)

	for _, rec := range records {
		ser := blockmanager.Serialize(rec)
		rSize := uint64(len(ser))
		d.numRecords++ // broj logičkih rekorda

		// 1. Ako staje u trenutni blok
		if curBlockBytes+rSize <= d.blockSize {
			if len(curRecords) == 0 {
				firstKeyInBlock = []byte(rec.GetKey())
			}
			curRecords = append(curRecords, rec)
			curBlockBytes += rSize
			continue
		}

		// 2. Ako staje u prazan blok (zatvori trenutni i otvori novi)
		if rSize <= d.blockSize {
			if len(curRecords) > 0 {
				// upiši trenutni blok
				d.blockManager.WriteBlock(curRecords, d.fileName, uint64(currentBlockNum))
				indexEntries = append(indexEntries, IndexEntry{
					Key:       append([]byte(nil), firstKeyInBlock...),
					DataBlock: currentBlockNum,
					Offset:    0,
				})
				currentBlockNum++
			}

			// novi blok
			curRecords = []*blockmanager.Record{rec}
			curBlockBytes = rSize
			firstKeyInBlock = []byte(rec.GetKey())
			continue
		}

		// 3. Ako je rekord veći od blockSize → podeli ga
		recParts := rec.DivideRecord(d.blockSize)
		for _, r := range recParts {
			serPart := blockmanager.Serialize(r)
			partSize := uint64(len(serPart))

			if partSize > d.blockSize {
				return nil, fmt.Errorf("fragment i dalje veći od blockSize (key=%s)", r.GetKey())
			}

			if curBlockBytes+partSize > d.blockSize {
				// zatvori trenutni blok
				d.blockManager.WriteBlock(curRecords, d.fileName, uint64(currentBlockNum))
				indexEntries = append(indexEntries, IndexEntry{
					Key:       append([]byte(nil), firstKeyInBlock...),
					DataBlock: currentBlockNum,
					Offset:    0,
				})
				currentBlockNum++
				curRecords = curRecords[:0]
				curBlockBytes = 0
				firstKeyInBlock = nil
			}

			if len(curRecords) == 0 {
				firstKeyInBlock = []byte(r.GetKey())
			}
			curRecords = append(curRecords, r)
			curBlockBytes += partSize
		}
	}

	// upiši poslednji data blok
	if len(curRecords) > 0 {
		d.blockManager.WriteBlock(curRecords, d.fileName, uint64(currentBlockNum))
		indexEntries = append(indexEntries, IndexEntry{
			Key:       append([]byte(nil), firstKeyInBlock...),
			DataBlock: currentBlockNum,
			Offset:    0,
		})
		currentBlockNum++
	}

	return indexEntries, nil
}

// ReadDataFile učitava ceo blok iz .data fajla
func (d *Data) ReadDataFile(blockNum uint32) ([]*blockmanager.Record, error) {
	f, err := os.Open(d.fileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// izračunaj offset bloka u fajlu (preskoči header)
	offset := int64(blockNum-1)*int64(d.blockSize) + int64(blockmanager.HEADER_SIZE)
	buf := make([]byte, d.blockSize)

	_, err = f.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}

	// deserijalizuj sve rekorde u tom bloku
	records := make([]*blockmanager.Record, 0)
	i := 0
	for i < len(buf) {
		rec, errCode := blockmanager.Deserialize(buf[i:])
		if errCode != 0 || rec == nil {
			break
		}
		records = append(records, rec)
		i += int(rec.GetRecordSize())
	}

	return records, nil
}

// FindInBlock pretražuje ključ unutar datog bloka -- ne target nego key
func (d *Data) FindInBlock(blockNum uint32, target []byte) (*blockmanager.Record, bool, error) {
	records, err := d.ReadDataFile(blockNum)
	if err != nil {
		return nil, false, err
	}

	for _, rec := range records {
		if rec.GetKey() == string(target) {
			return rec, true, nil
		}
	}
	return nil, false, nil
}
