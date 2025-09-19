package sstable

import (
	"fmt"
	"os"
	"project/blockmanager"
)

// KV predstavlja sortirani key/value iz MemTable-a
type KV struct {
	Key       []byte
	Value     []byte
	Tombstone bool
}

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
func NewData(fileName string, blockSize uint64) *Data {
	return &Data{
		fileName:     fileName,
		blockSize:    blockSize,
		blockManager: blockmanager.NewBlockManager(blockmanager.NewBufferPool(), blockSize, 512),
		numRecords:   0,
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

// WriteDataFile upisuje sortedKv u dataFile koristeci blockSize.
// Vraca slice IndexEntry — po jedan entry za svaki napunjen data blok (prvi key u bloku).
func (d *Data) WriteDataFile(sortedKv []KV) (indexEntries []IndexEntry, err error) {
	// napisi header (blok 0)
	blockmanager.WriteHeader(d.fileName, d.blockSize)

	if len(sortedKv) == 0 {
		return nil, nil
	}

	var (
		currentBlockNum uint32 = 1
		curRecords             = make([]*blockmanager.Record, 0, 64)
		curBlockBytes   uint64 = 0
		firstKeyInBlock []byte
	)

	for _, kv := range sortedKv {
		var tbstn uint8 = 0
		if kv.Tombstone {
			tbstn = 1
		}

		// kreiramo osnovni record
		rec := blockmanager.SetRec(1, 0, tbstn, uint64(len(kv.Key)), uint64(len(kv.Value)), string(kv.Key), kv.Value)

		// podelimo record na fragmente koji staju u blok
		recParts := rec.DivideRecord(d.blockSize)

		for _, r := range recParts {
			ser := blockmanager.Serialize(r)
			rSize := uint64(len(ser))

			if curBlockBytes+rSize > d.blockSize {
				if len(curRecords) == 0 {
					return nil, fmt.Errorf("a single record fragment is larger than blockSize (key=%s)", string(kv.Key))
				}

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
				firstKeyInBlock = append([]byte(nil), kv.Key...)
			}

			curRecords = append(curRecords, r)
			curBlockBytes += rSize
			d.numRecords++
		}
	}

	if len(curRecords) > 0 {
		d.blockManager.WriteBlock(curRecords, d.fileName, uint64(currentBlockNum))
		indexEntries = append(indexEntries, IndexEntry{
			Key:       append([]byte(nil), firstKeyInBlock...),
			DataBlock: currentBlockNum,
			Offset:    0,
		})
	}

	return indexEntries, nil
}

// ReadBlock ucitava ceo blok iz .data fajla
func (d *Data) ReadDataFile(blockNum uint32) ([]*blockmanager.Record, error) {
	f, err := os.Open(d.fileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// izracunaj offset bloka u fajlu (preskoci header)
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

// FindInBlock pretrazuje kljuc unutar datog bloka
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
