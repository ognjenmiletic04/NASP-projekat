/*
func Serialize(r *Record) []byte - funkcija za serijalizaciju rekorda vraca niz bajtova

func Deserialize(blockData []byte) (*Record, uint8)- deserijalizacija rekorda, vraca rekord i porucu o gresci, 1 greska, 0 nema greske

func SetRec(tip uint16, lognum uint64, tbstn uint8, ks uint64, vs uint64, k string, v []byte) *Record - pravi rekord za zadate parametre

func RecordPart(wholeRecord *Record, valueSize uint64, valueChunk []byte, part uint16) *Record

func RecordsToByte(records []*Record) []byte

func (record *Record) DivideRecord(blockSize uint64) []*Record
*/
package blockmanager

import (
	"encoding/binary"
	"log"
	"time"
)

type Record struct {
	crcData    uint32
	logNum     uint64
	recordType uint16 //full, first, middle, last ; 1, 2, 3
	timeStamp  uint64
	tombstone  uint8
	keySize    uint64
	valueSize  uint64
	key        string
	value      []byte
	recordSize uint64
}

func (record *Record) GetCrcData() uint32 {
	return record.crcData
}
func (record *Record) GetLogNum() uint64 {
	return record.logNum
}
func (record *Record) GetRecordType() uint16 {
	return record.recordType
}
func (record *Record) GetTimeStamp() uint64 {
	return record.timeStamp
}
func (record *Record) GetTombstone() uint8 {
	return record.tombstone
}
func (record *Record) GetKeySize() uint64 {
	return record.keySize
}
func (record *Record) GetValueSize() uint64 {
	return record.valueSize
}
func (record *Record) GetKey() string {
	return record.key
}
func (record *Record) GetValue() []byte {
	return record.value
}
func (record *Record) GetRecordSize() uint64 {
	return record.recordSize
}

func (r *Record) SetCRCData(crc uint32) {
	r.crcData = crc
}

func (r *Record) SetLogNum(logNum uint64) {
	r.logNum = logNum
}

func (r *Record) SetRecordType(recordType uint16) {
	r.recordType = recordType
}

func (r *Record) SetTimeStamp(timeStamp uint64) {
	r.timeStamp = timeStamp
}

func (r *Record) SetTombstone(tombstone uint8) {
	r.tombstone = tombstone
}

func (r *Record) SetKeySize(keySize uint64) {
	r.keySize = keySize
}

func (r *Record) SetValueSize(valueSize uint64) {
	r.valueSize = valueSize
}

func (r *Record) SetKey(key string) {
	r.key = key
}

func (r *Record) SetValue(value []byte) {
	r.value = value
}

func (r *Record) SetRecordSize(recordSize uint64) {
	r.recordSize = recordSize
}

func Serialize(r *Record) []byte {
	data := make([]byte, 0)

	crcByte := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcByte, r.crcData)

	recordSizeByte := make([]byte, 8)
	binary.LittleEndian.PutUint64(recordSizeByte, r.recordSize)

	recordTypeByte := make([]byte, 2)
	binary.LittleEndian.PutUint16(recordTypeByte, r.recordType)

	logNumByte := make([]byte, 8)
	binary.LittleEndian.PutUint64(logNumByte, r.logNum)

	timeStampByte := make([]byte, 8)
	binary.LittleEndian.PutUint64(timeStampByte, r.timeStamp)

	keySizeByte := make([]byte, 8)
	binary.LittleEndian.PutUint64(keySizeByte, r.keySize)

	valueSizeByte := make([]byte, 8)
	binary.LittleEndian.PutUint64(valueSizeByte, r.valueSize)

	data = append(data, crcByte...)
	data = append(data, recordSizeByte...)
	data = append(data, recordTypeByte...)
	data = append(data, logNumByte...)
	data = append(data, timeStampByte...)
	data = append(data, byte(r.tombstone))
	data = append(data, keySizeByte...)
	data = append(data, valueSizeByte...)
	data = append(data, []byte(r.key)...)
	data = append(data, r.value...)

	return data
}

func Deserialize(blockData []byte) (*Record, uint8) {
	r := &Record{}
	data := make([]byte, 0)
	start := 0
	if len(blockData) < start+4 {
		return nil, 1
	}
	crcByte := blockData[start : start+4]
	start += 4

	r.crcData = binary.LittleEndian.Uint32(crcByte)
	if r.crcData == 0 {
		return nil, 1
	}

	if len(blockData) < start+8 {
		return nil, 1
	}
	recordSizeByte := blockData[start : start+8]
	start += 8

	if len(blockData) < start+2 {
		return nil, 1
	}
	recordTypeByte := blockData[start : start+2]
	start += 2

	if len(blockData) < start+8 {
		return nil, 1
	}
	logNumByte := blockData[start : start+8]
	start += 8

	if len(blockData) < start+8 {
		return nil, 1
	}
	timeStampByte := blockData[start : start+8]
	start += 8

	if len(blockData) < start+1 {
		return nil, 1
	}
	tombstoneByte := blockData[start : start+1]
	start += 1

	if len(blockData) < start+8 {
		return nil, 1
	}
	keySizeByte := blockData[start : start+8]
	start += 8

	if len(blockData) < start+8 {
		return nil, 1
	}
	valueSizeByte := blockData[start : start+8]
	start += 8

	r.recordSize = binary.LittleEndian.Uint64(recordSizeByte)
	r.logNum = binary.LittleEndian.Uint64(logNumByte)
	r.recordType = binary.LittleEndian.Uint16(recordTypeByte)
	r.timeStamp = binary.LittleEndian.Uint64(timeStampByte)
	r.tombstone = tombstoneByte[0]
	r.keySize = binary.LittleEndian.Uint64(keySizeByte)
	r.valueSize = binary.LittleEndian.Uint64(valueSizeByte)

	if len(blockData) < start+int(r.keySize) {
		return nil, 1
	}
	keyByte := blockData[start : start+int(r.keySize)]
	start += int(r.keySize)

	if len(blockData) < start+int(r.valueSize) {
		return nil, 1
	}
	valueByte := blockData[start : start+int(r.valueSize)]
	start += int(r.valueSize)

	r.key = string(keyByte)
	r.value = valueByte

	data = append(data, recordSizeByte...)
	data = append(data, recordTypeByte...)
	data = append(data, logNumByte...)
	data = append(data, timeStampByte...)
	data = append(data, byte(r.tombstone))
	data = append(data, keySizeByte...)
	data = append(data, valueSizeByte...)
	data = append(data, []byte(r.key)...)
	data = append(data, r.value...)
	if r.crcData != CRC32(data) {
		panic("Greska crc se ne poklapa")
	}

	return r, 0
}

func RecordsToByte(records []*Record) []byte {
	data := make([]byte, 0)
	for _, record := range records {
		recordByte := Serialize(record)
		data = append(data, recordByte...)
	}
	return data
}

func SetRec(tip uint16, lognum uint64, tbstn uint8, ks uint64, vs uint64, k string, v []byte) *Record {
	r := &Record{}
	vreme := time.Now()
	r.timeStamp = uint64(vreme.Unix())
	r.recordType = tip
	r.logNum = lognum
	r.tombstone = tbstn
	if len(k) > 3000000 {
		log.Fatal("Key size is too big")
	}
	r.keySize = ks
	r.valueSize = vs
	r.key = k
	r.value = v
	r.recordSize = 4 + 8 + 2 + 8 + 8 + 1 + 8 + 8 + r.keySize + r.valueSize

	data := make([]byte, 0)
	recordSizeByte := make([]byte, 8)
	binary.LittleEndian.PutUint64(recordSizeByte, r.recordSize)

	recordTypeByte := make([]byte, 2)
	binary.LittleEndian.PutUint16(recordTypeByte, r.recordType)

	logByte := make([]byte, 8)
	binary.LittleEndian.PutUint64(logByte, r.logNum)

	tsByte := make([]byte, 8)
	binary.LittleEndian.PutUint64(tsByte, uint64(r.timeStamp))

	ksByte := make([]byte, 8)
	binary.LittleEndian.PutUint64(ksByte, uint64(r.keySize))

	vsByte := make([]byte, 8)
	binary.LittleEndian.PutUint64(vsByte, uint64(r.valueSize))

	data = append(data, recordSizeByte...)
	data = append(data, recordTypeByte...)
	data = append(data, logByte...)
	data = append(data, tsByte...)
	data = append(data, byte(r.tombstone))
	data = append(data, ksByte...)
	data = append(data, vsByte...)
	data = append(data, []byte(r.key)...)
	data = append(data, r.value...)
	r.crcData = CRC32(data)
	return r
}
func (record *Record) DivideRecord(blockSize uint64) []*Record {
	records := make([]*Record, 0)
	recordBaseSize := RECORD_BASE_SIZE + record.keySize
	spaceLeftInBlockValue := blockSize - uint64(recordBaseSize)
	numOfIterNewRecords := len(record.value)/int(spaceLeftInBlockValue) + 1 //+1 ako je ostalo nesto posto je celobrojno deljenje
	lenRecordValue := len(record.value)
	valueChunk := record.value

	point := spaceLeftInBlockValue

	i := 0
	for i != numOfIterNewRecords {
		if lenRecordValue == 0 {
			records[len(records)-1].recordType = 3
			break
		}
		if lenRecordValue/int(spaceLeftInBlockValue) < 1 {
			point = uint64(lenRecordValue)
			records = append(records, RecordPart(record, uint64(lenRecordValue), valueChunk[:point], 3))
			break
		}
		if i == 0 {
			records = append(records, RecordPart(record, spaceLeftInBlockValue, valueChunk[:point], 1))
		} else {
			records = append(records, RecordPart(record, spaceLeftInBlockValue, valueChunk[:point], 2))
		}
		lenRecordValue -= int(spaceLeftInBlockValue)
		valueChunk = valueChunk[point:]

		i++
	}
	return records
}

func RecordPart(wholeRecord *Record, valueSize uint64, valueChunk []byte, part uint16) *Record {
	record := SetRec(part, wholeRecord.logNum, wholeRecord.tombstone, wholeRecord.keySize, valueSize, wholeRecord.key, valueChunk)
	return record
}
