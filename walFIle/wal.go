/*
Postoje geteri i seteri za sve atribute wal strukutre

func NewWal(blockNum uint64, blockManager *blockmanager.BlockManager) *WAL - pravi se novi wal

func (wal *WAL) WriteRecord(record *blockmanager.Record, blockManager *blockmanager.BlockManager) - pisanje rekorda u wal

func (wal *WAL) CreateSegment(blockManager *blockmanager.BlockManager) - pravljenje novog segmenta

func (wal *WAL) LoadSegments() - ucitavanje svih segmenata, poziva se u funkciji newwal

func (wal *WAL) NextRecord(blockManager *blockmanager.BlockManager) *blockmanager.Record -funkcija koja ide redom i cita rekord jedan po jedan
jedina funkcija bi trebala da bude, vracanje stanja, kada se ucitaju segmenti kada se pokrene wal da se ide redom sa ovom funkcijom i da se izvrsavaju operacije
nema posebne funkcije koja to radi, ali samo se pokrene beskonacna petlja i izvrte se svi rekordi.

func (wal *WAL) ResetCounter()- pomocna funkcija da NextRecord funkcija krene od pocetka

func (wal *WAL) DeleteSegments(index uint64) - brisu se svi segmenti ciji je broj manji od
*/

package wal

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"project/blockmanager"
	"sort"
	"strconv"
	"strings"
)

type WAL struct {
	blockNumber       uint64
	segmentFilePaths  []string
	activeSegmentPath string //aktivan segment za pisanje
	blockManager      *blockmanager.BlockManager
	numberofRecords   uint64

	currentRecordIndex         uint64 //pise u dokumentaciji da wal cita rekord po rekord pa mi treba ovo
	currentRecordBlockNum      uint64
	currentRecordFilePath      string
	currentRecordFilePathIndex uint64
}

// Setters for WAL struct
func (wal *WAL) SetBlockNumber(num uint64) {
	wal.blockNumber = num
}

func (wal *WAL) SetSegmentFilePaths(paths []string) {
	wal.segmentFilePaths = paths
}

func (wal *WAL) SetActiveSegmentPath(path string) {
	wal.activeSegmentPath = path
}

func (wal *WAL) SetBlockManager(manager *blockmanager.BlockManager) {
	wal.blockManager = manager
}

func (wal *WAL) SetNumberOfRecords(num uint64) {
	wal.numberofRecords = num
}

func (wal *WAL) SetCurrentRecordIndex(idx uint64) {
	wal.currentRecordIndex = idx
}

func (wal *WAL) SetCurrentRecordBlockNum(num uint64) {
	wal.currentRecordBlockNum = num
}

func (wal *WAL) SetCurrentRecordFilePath(path string) {
	wal.currentRecordFilePath = path
}

func (wal *WAL) SetCurrentRecordFilePathIndex(idx uint64) {
	wal.currentRecordFilePathIndex = idx
}

func (wal *WAL) GetBlockNum() uint64 {
	return wal.blockNumber
}
func (wal *WAL) GetSegmentFilePaths() []string {
	return wal.segmentFilePaths
}
func (wal *WAL) GetActiveSegmentPath() string {
	return wal.activeSegmentPath
}
func (wal *WAL) GetBlockManager() *blockmanager.BlockManager {
	return wal.blockManager
}
func (wal *WAL) GetNumberOfRecords() uint64 {
	return wal.numberofRecords
}
func (wal *WAL) GetCurrentRecordIndex() uint64 {
	return wal.currentRecordIndex
}
func (wal *WAL) GetCurrentRecordBlockNum() uint64 {
	return wal.currentRecordBlockNum
}
func (wal *WAL) GetCurrentRecordFilePath() string {
	return wal.currentRecordFilePath
}
func (wal *WAL) GetCurrentRecordFilePathIndex() uint64 {
	return wal.currentRecordFilePathIndex
}

func (wal *WAL) WriteRecord(record *blockmanager.Record, blockManager *blockmanager.BlockManager) error {
	file, err := os.OpenFile(wal.activeSegmentPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %v", err)
	}
	defer file.Close()

	end, err := file.Seek(0, 2)
	if err != nil {
		return fmt.Errorf("failed to seek to end of WAL file: %v", err)
	}
	lastBlockNumber := (end - blockmanager.HEADER_SIZE) / int64(blockManager.GetBlockSize())

	block := blockManager.ReadBlock(wal.activeSegmentPath, uint64(lastBlockNumber))

	recordsSizeSum := uint64(0)
	for _, record := range block.GetRecords() {
		recordsSizeSum += record.GetRecordSize()
	}
	spaceLeft := blockManager.GetBlockSize() - recordsSizeSum
	records := block.GetRecords()
	//slucaj za deljenje rekorda

	if len(records) == 0 && spaceLeft < record.GetRecordSize() {
		dividedRecords := record.DivideRecord(blockManager.GetBlockSize())
		for _, rec := range dividedRecords {
			err := wal.WriteRecord(rec, blockManager)
			if err != nil {
				return fmt.Errorf("failed to write divided record: %v", err)
			}
		}
		return nil
	}

	if spaceLeft >= record.GetRecordSize() {

		records = append(records, record)
		block.SetRecords(records)
		wal.numberofRecords++
		return nil
	} else if lastBlockNumber+1 <= int64(wal.blockNumber) {
		block := blockManager.ReadBlock(wal.activeSegmentPath, uint64(lastBlockNumber+1))
		recordsSizeSum = uint64(0)
		for _, record := range block.GetRecords() {
			recordsSizeSum += record.GetRecordSize()
		}
		spaceLeft = blockManager.GetBlockSize() - recordsSizeSum
		records := block.GetRecords()
		//slucaj za deljenje rekorda

		if len(records) == 0 && spaceLeft < record.GetRecordSize() {
			dividedRecords := record.DivideRecord(blockManager.GetBlockSize())
			for _, rec := range dividedRecords {
				err := wal.WriteRecord(rec, blockManager)
				if err != nil {
					return fmt.Errorf("failed to write divided record: %v", err)
				}
			}
			return nil
		}
		// records = block.GetRecords()
		records = append(records, record)
		block.SetRecords(records)
		wal.numberofRecords++
		return nil
	} else {
		err := wal.CreateSegment(wal.blockManager)
		if err != nil {
			return fmt.Errorf("failed to create new segment: %v", err)
		}
		block := blockManager.ReadBlock(wal.activeSegmentPath, 1)
		records = block.GetRecords()
		records = append(records, record)
		block.SetRecords(records)
		wal.numberofRecords++
		return nil
	}
}

func (wal *WAL) CreateSegment(blockManager *blockmanager.BlockManager) error {
	if len(wal.segmentFilePaths) == 0 {
		err := os.MkdirAll("walFile/WAL", 0755)
		if err != nil {
			return fmt.Errorf("failed to create WAL directory: %v", err)
		}
		newName := "walFile/WAL/wal_001.log"
		file, err := os.Create(newName)
		if err != nil {
			return fmt.Errorf("failed to create WAL file: %v", err)
		}
		file.Close()
		wal.activeSegmentPath = newName
		wal.segmentFilePaths = append(wal.segmentFilePaths, newName)
		blockmanager.WriteHeader(newName, blockManager.GetBlockSize())
		wal.ResetCounter()
		return nil
	}
	lastSegmentName := wal.segmentFilePaths[len(wal.segmentFilePaths)-1]
	segmentNumberStr := strings.Split(strings.Split(lastSegmentName, "_")[1], ".")[0]
	segmentNumber, err := strconv.Atoi(segmentNumberStr)
	if err != nil {
		return fmt.Errorf("invalid segment number format: %v", err)
	}
	newName := fmt.Sprintf("wal_%03d.log", segmentNumber+1)
	file, err := os.Create("walFile/WAL/" + newName)
	if err != nil {
		return fmt.Errorf("failed to create new WAL segment: %v", err)
	}
	file.Close()
	wal.activeSegmentPath = "walFile/WAL/" + newName
	wal.segmentFilePaths = append(wal.segmentFilePaths, "walFile/WAL/"+newName)
	blockmanager.WriteHeader(wal.activeSegmentPath, blockManager.GetBlockSize())
	wal.ResetCounter()
	return nil
}

func NewWal(blockNum uint64, blockManager *blockmanager.BlockManager) *WAL {
	wal := &WAL{blockNumber: blockNum, currentRecordIndex: 0, currentRecordBlockNum: 1, currentRecordFilePathIndex: 0, blockManager: blockManager, numberofRecords: 0}
	wal.LoadSegments()
	if len(wal.activeSegmentPath) == 0 {
		err := wal.CreateSegment(blockManager)
		if err != nil {
			log.Fatalf("Failed to create initial WAL segment: %v", err)
		}
	}
	wal.currentRecordFilePath = wal.segmentFilePaths[0]
	return wal
}

func (wal *WAL) LoadSegments() {

	wal.numberofRecords = 0
	os.MkdirAll("walFile/WAL", 0755)
	wal.segmentFilePaths = make([]string, 0)
	entries, err := os.ReadDir("walFile/WAL")
	if err != nil {
		log.Fatal(err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			filePath := "walFile/WAL/" + entry.Name()
			wal.segmentFilePaths = append(wal.segmentFilePaths, filePath)
		}
	}
	sort.Strings(wal.segmentFilePaths)

	if len(wal.segmentFilePaths) == 0 {
		wal.currentRecordFilePath = ""
		wal.currentRecordFilePathIndex = 0
		wal.currentRecordBlockNum = 1
		wal.currentRecordIndex = 0
		return
	}
	wal.activeSegmentPath = wal.segmentFilePaths[len(wal.segmentFilePaths)-1]
	wal.ResetCounter()

	for {
		rec := wal.NextRecord(wal.blockManager)
		if rec == nil {
			break
		}
		wal.numberofRecords++
	}
}
func (wal *WAL) ResetCounter() {
	if len(wal.segmentFilePaths) == 0 {
		wal.currentRecordFilePathIndex = 0
		wal.currentRecordBlockNum = 1
		wal.currentRecordIndex = 0
		wal.currentRecordFilePath = ""
		return
	}

	wal.currentRecordFilePathIndex = 0
	wal.currentRecordBlockNum = 1
	wal.currentRecordIndex = 0
	wal.currentRecordFilePath = wal.segmentFilePaths[0]
}
func (wal *WAL) NextRecord(blockManager *blockmanager.BlockManager) *blockmanager.Record {
	if len(wal.segmentFilePaths) == 0 {
		return nil
	}

	if wal.currentRecordFilePath == "" {
		return nil
	}

	tempBlockManager := blockManager //ako je fajl pisan sa drugacijom velicinom bloka
	header := blockmanager.ReadHeader(wal.currentRecordFilePath)
	if header != nil {
		for _, record := range header.GetRecords() {
			if record.GetKey() == "block size" {
				valueUint := binary.LittleEndian.Uint64(record.GetValue())
				if valueUint != blockManager.GetBlockSize() {
					tempBlockManager = blockmanager.NewBlockManager(blockManager.GetBufferPool(), valueUint, blockManager.GetBufferPoolSize())
					break
				}
			}
		}
	}
	block := tempBlockManager.ReadBlock(wal.currentRecordFilePath, wal.currentRecordBlockNum)
	if block == nil {
		return nil
	}
	if len(block.GetRecords()) == 0 {
		if wal.currentRecordFilePathIndex >= uint64(len(wal.segmentFilePaths)) {
			return nil
		}
		wal.currentRecordFilePathIndex = 0
		wal.currentRecordFilePath = wal.segmentFilePaths[wal.currentRecordFilePathIndex]
		wal.currentRecordBlockNum = 1
		wal.currentRecordIndex = 0
		return nil
	}
	record := block.GetRecords()[wal.currentRecordIndex]

	if record.GetRecordType() == 1 {
		record = wal.ConnectDividedRecord(record, block)
	}

	if wal.currentRecordIndex+1 < uint64(len(block.GetRecords())) {
		wal.currentRecordIndex++
	} else if wal.currentRecordBlockNum+1 <= wal.blockNumber { //zbog hedera ide <=
		wal.currentRecordBlockNum++
		wal.currentRecordIndex = 0
	} else if wal.currentRecordFilePathIndex+1 < uint64(len(wal.segmentFilePaths)) {
		wal.currentRecordFilePathIndex++
		if wal.currentRecordFilePathIndex < uint64(len(wal.segmentFilePaths)) {
			wal.currentRecordFilePath = wal.segmentFilePaths[wal.currentRecordFilePathIndex] //nije potreban moze i samo sa indeksom
		}
		wal.currentRecordBlockNum = 1
		wal.currentRecordIndex = 0
	} else {

		return nil
	}

	return record

}

func (wal *WAL) ConnectDividedRecord(firstPart *blockmanager.Record, currentBlock *blockmanager.Block) *blockmanager.Record {
	record := firstPart
	block := currentBlock
	value := make([]byte, 0)
	value = append(value, record.GetValue()...)
	for record.GetRecordType() != 3 {
		if wal.currentRecordIndex+1 < uint64(len(block.GetRecords())) {
			wal.currentRecordIndex++
			record = block.GetRecords()[wal.currentRecordIndex]
			value = append(value, record.GetValue()...)
		} else if wal.currentRecordBlockNum+1 <= wal.blockNumber { //zbog hedera ide <=
			wal.currentRecordBlockNum++
			wal.currentRecordIndex = 0
			block = wal.blockManager.ReadBlock(wal.currentRecordFilePath, wal.currentRecordBlockNum)
			record = block.GetRecords()[wal.currentRecordIndex]
			value = append(value, record.GetValue()...)
		} else if wal.currentRecordFilePathIndex+1 < uint64(len(wal.segmentFilePaths)) {
			wal.currentRecordFilePathIndex++
			wal.currentRecordFilePath = wal.segmentFilePaths[wal.currentRecordFilePathIndex] //nije potreban moze i samo sa indeksom
			wal.currentRecordBlockNum = 1
			wal.currentRecordIndex = 0
			block = wal.blockManager.ReadBlock(wal.currentRecordFilePath, wal.currentRecordBlockNum)
			record = block.GetRecords()[wal.currentRecordIndex]
			value = append(value, record.GetValue()...)
		} else if wal.currentRecordFilePathIndex+1 >= uint64(len(wal.segmentFilePaths)) {
			wal.currentRecordFilePathIndex = 0
			wal.currentRecordFilePath = wal.segmentFilePaths[wal.currentRecordFilePathIndex]
			wal.currentRecordBlockNum = 1
			wal.currentRecordIndex = 0
			fmt.Println("Error record parts are missing")
		}

	}
	//return blockmanager.SetRec(0, firstPart.GetLogNum(), firstPart.GetTombstone(), firstPart.GetKeySize(), uint64(len(value)), firstPart.GetKey(), value)
	// Rekonstruišemo finalni FULL record (recordType = 0) bez menjanja originalnog timestamp-a
	// NE koristimo SetRec jer bi on kreirao novi timestamp i time narušio redosled verzija pri replay-u WAL-a
	r := &blockmanager.Record{}
	r.SetTimeStamp(firstPart.GetTimeStamp())
	r.SetRecordType(0)
	r.SetLogNum(firstPart.GetLogNum())
	r.SetTombstone(firstPart.GetTombstone())
	r.SetKeySize(firstPart.GetKeySize())
	r.SetValueSize(uint64(len(value)))
	r.SetKey(firstPart.GetKey())
	r.SetValue(value)
	r.SetRecordSize(4 + 8 + 2 + 8 + 8 + 1 + 8 + 8 + r.GetKeySize() + r.GetValueSize())

	// Izračun CRC (isti način kao u SetRec / RecordPart)
	data := make([]byte, 0)
	recordSizeByte := make([]byte, 8)
	binary.LittleEndian.PutUint64(recordSizeByte, r.GetRecordSize())
	recordTypeByte := make([]byte, 2)
	binary.LittleEndian.PutUint16(recordTypeByte, uint16(r.GetRecordType()))
	logByte := make([]byte, 8)
	binary.LittleEndian.PutUint64(logByte, r.GetLogNum())
	tsByte := make([]byte, 8)
	binary.LittleEndian.PutUint64(tsByte, r.GetTimeStamp())
	ksByte := make([]byte, 8)
	binary.LittleEndian.PutUint64(ksByte, r.GetKeySize())
	vsByte := make([]byte, 8)
	binary.LittleEndian.PutUint64(vsByte, r.GetValueSize())
	data = append(data, recordSizeByte...)
	data = append(data, recordTypeByte...)
	data = append(data, logByte...)
	data = append(data, tsByte...)
	data = append(data, byte(r.GetTombstone()))
	data = append(data, ksByte...)
	data = append(data, vsByte...)
	data = append(data, []byte(r.GetKey())...)
	data = append(data, r.GetValue()...)
	r.SetCRCData(blockmanager.CRC32(data))
	return r
	// logNum je isti kao kod firstPart; svi delovi dele isti logNum tako da je deterministično.
}

func (wal *WAL) DeleteSegments(index uint64) {
	if wal.blockManager.GetBufferPool() != nil {
		wal.blockManager.EmptyBufferPool()
	}
	for _, segment := range wal.segmentFilePaths {
		segmentNumberStr := strings.Split(strings.Split(segment, "_")[1], ".")[0]
		segmentNumber, err := strconv.Atoi(segmentNumberStr)
		if err != nil {
			panic("Invalid segment number format")
		}
		if uint64(segmentNumber) < index {
			if err := os.Remove(segment); err != nil {
				fmt.Printf("DeleteSegments: failed to remove %s: %v\n", segment, err)
			}
		}
	}
	wal.LoadSegments()
}
