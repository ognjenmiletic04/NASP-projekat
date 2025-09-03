/*
5 struktura Record, Block, BlockManager, BufferPool
Za svaku strukturu postoje geteri i seteri, geteri-GetAtribut, seteri-SetAtribut
BufferPool i BlockManager strukture se prave sa funkcijama NewBufferPool i NewBlockManager

func (blockManager *BlockManager) ReadBlock(fileName string, blockNum uint64) *Block - citanje bloka, vraca pokazivac na procitani blok, za prosledjeno ime fajla i broj bloka
ako se u fajlu nalazi 2 upisana bloka i pokusa se citanje sledeceg bloka, vraca se novi prazan blok koji se i upisuje u fajl. Jako bitna
stvar broj bloka uvek krece od 1 ne od 0, nulti je heder

func (blockManager *BlockManager) WriteBlock(records []*Record, fileName string, blockNum uint64) - pisanje bloka uglavno ne bi trebalo da se
ni poziva eksplicitno posto se funkcija poziva kada se isprazni buferpul.

func (bufferPool *BufferPool) CheckForBlock(blockNum uint64, filePath string) *Block-

func (blockManager *BlockManager) CheckPoolCapacity() bool

func ReadHeader(fileName string) *Block - kada god se cita stari fajl obavezno se prvo poziva ova funkcija koja vraca pokazivac na headerblok
u kom se nalaze (za sada samo) podaci o velicini bloka, da bi fajl mogao da se procita pravi se novi privremeni blok menadzer sa tom velicinom
bloka koja je procitana i sa tim privremeni blok menadzerom se cita taj fajl

func WriteHeader(fileName string, blockSize uint64) -kada god se pravi neki novi fajl obavezno se odma nakon sto je napravljen poziva ova funkcija
da bi se zapisao(za sada samo) broj bloka da bi mogao da se procita fajl ako se broj bloka promeni

func (blockManager *BlockManager) EmptyBufferPool()

func (blockManager *BlockManager) CheckPoolCapacity() bool
*/

package blockmanager

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const (
	CRC_SIZE         = 4
	REC_SIZE         = 8
	TYPE             = 2
	LOG_NUMBER       = 8
	TIMESTAMP_SIZE   = 8
	TOMBSTONE_SIZE   = 1
	KEY_SIZE_SIZE    = 8
	VALUE_SIZE_SIZE  = 8
	MAX_KEY_SIZE     = 3000000
	RECORD_BASE_SIZE = CRC_SIZE + REC_SIZE + TYPE + LOG_NUMBER + TIMESTAMP_SIZE + TOMBSTONE_SIZE + KEY_SIZE_SIZE + VALUE_SIZE_SIZE
	HEADER_SIZE      = 512
)

type BlockManager struct {
	bufferPool     *BufferPool
	blockSize      uint64
	bufferPoolSize uint64
}

type BufferPool struct {
	blocks []*Block
}

// geteri=================================================

func (blockManager *BlockManager) GetBufferPool() *BufferPool {
	return blockManager.bufferPool
}
func (blockManager *BlockManager) GetBlockSize() uint64 {
	return blockManager.blockSize
}
func (blockManager *BlockManager) GetBufferPoolSize() uint64 {
	return blockManager.bufferPoolSize
}
func (bufferPool *BufferPool) GetBlocks() []*Block {
	return bufferPool.blocks
}

// seteri=====================================================

func (bm *BlockManager) SetBufferPool(bp *BufferPool) {
	bm.bufferPool = bp
}

func (bm *BlockManager) SetBlockSize(size uint64) {
	bm.blockSize = size
}

func (bm *BlockManager) SetBufferPoolSize(size uint64) {
	bm.bufferPoolSize = size
}

func (bp *BufferPool) SetBlocks(blocks []*Block) {
	bp.blocks = blocks
}

// ===========================================================================================
func NewBlockManager(bp *BufferPool, blockSize uint64, poolSize uint64) *BlockManager {
	bm := &BlockManager{}
	bm.bufferPool = bp
	bm.blockSize = blockSize
	bm.bufferPoolSize = poolSize
	return bm
}
func NewBufferPool() *BufferPool {
	bp := &BufferPool{}
	return bp
}

func (blockManager *BlockManager) ReadBlock(fileName string, blockNum uint64) *Block {
	block := blockManager.bufferPool.CheckForBlock(blockNum, fileName)
	if block != nil {
		return block
	}
	file, err := os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		fmt.Printf("Error with opening file")
		return nil
	}
	defer file.Close()
	if blockNum != 0 {
		file.Seek((int64(blockNum)-1)*int64(blockManager.blockSize)+HEADER_SIZE, io.SeekStart)
	} else {
		blockNum++
		file.Seek(HEADER_SIZE, 0)
	}

	data := make([]byte, blockManager.blockSize)
	end, _ := file.Read(data)
	if end == 0 {
		newBlock := &Block{blockFilePath: fileName, blockNumber: blockNum}
		blockManager.bufferPool.blocks = append(blockManager.bufferPool.blocks, newBlock)
		flushed := blockManager.CheckPoolCapacity()
		if !flushed {
			blockManager.WriteBlock(newBlock.records, fileName, newBlock.blockNumber) //cisto da se zauzme prostor posto ce se kasnije isprazniti ali ako ne napisem makar prazan blok zabosce brojenje blokova, nije napisan pa se ni ne broji ali je u bafer pulu
		}
		return blockManager.ReadBlock(fileName, blockNum) //ako dodavanje pokrene praznjenje baferpula mora da se pozove readblock opet da bi se procitao i vratio
	}

	block = &Block{}
	blockManager.bufferPool.blocks = append(blockManager.bufferPool.blocks, block)
	block.blockNumber = blockNum
	block.blockFilePath = fileName
	start := 0
	for {
		record, err := Deserialize(data[start:])
		if err != 0 {
			break
		}
		block.records = append(block.records, record)
		start += int(record.recordSize)
	}
	blockManager.CheckPoolCapacity()
	return block
}
func (bufferPool *BufferPool) CheckForBlock(blockNum uint64, filePath string) *Block {
	for _, block := range bufferPool.blocks {
		if block.blockNumber == blockNum && block.blockFilePath == filePath {
			return block
		}
	}
	return nil
}
func (blockManager *BlockManager) WriteBlock(records []*Record, fileName string, blockNum uint64) {
	data := RecordsToByte(records)
	file, err := os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		fmt.Printf("Error with opening file")
		return
	}
	defer file.Close()
	if blockNum != 0 {
		file.Seek((int64(blockNum)-1)*int64(blockManager.blockSize)+HEADER_SIZE, io.SeekStart)
	}
	file.Write(data)
	if len(data) < int(blockManager.blockSize) {
		padding := make([]byte, blockManager.blockSize-uint64(len(data)))
		file.Write(padding)
	}
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
func (blockManager *BlockManager) CheckPoolCapacity() bool {
	poolCapacity := 0
	for range blockManager.bufferPool.blocks {
		poolCapacity += int(blockManager.blockSize)
	}
	if poolCapacity > int(blockManager.bufferPoolSize) {
		blockManager.EmptyBufferPool()
	}
	return poolCapacity > int(blockManager.bufferPoolSize)
}
func (blockManager *BlockManager) EmptyBufferPool() {
	for _, block := range blockManager.bufferPool.blocks {
		blockManager.WriteBlock(block.records, block.blockFilePath, block.blockNumber)
	}
	blockManager.bufferPool.blocks = make([]*Block, 0)
}

func ReadHeader(fileName string) *Block {
	blockManagerHeader := NewBlockManager(NewBufferPool(), 512, 512)
	return blockManagerHeader.ReadBlockHeader(fileName, 0)
}

func (blockManager *BlockManager) ReadBlockHeader(fileName string, blockNum uint64) *Block {
	block := blockManager.bufferPool.CheckForBlock(blockNum, fileName)
	if block != nil {
		return block
	}
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Printf("Error with opening file")
		return nil
	}
	defer file.Close()

	data := make([]byte, blockManager.blockSize)
	file.Read(data)

	block = &Block{}
	blockManager.bufferPool.blocks = append(blockManager.bufferPool.blocks, block)
	block.blockNumber = blockNum
	block.blockFilePath = fileName
	start := 0
	for {
		record, err := Deserialize(data[start:])
		// littleEndianUint64 := binary.LittleEndian.Uint64(record.value)
		// fmt.Print(littleEndianUint64)
		if err != 0 {
			break
		}
		block.records = append(block.records, record)
		start += int(record.recordSize)
	}
	blockManager.CheckPoolCapacity()
	return block
}

func WriteHeader(fileName string, blockSize uint64) {
	blockManagerHeader := NewBlockManager(NewBufferPool(), 512, 512)
	data := make([]*Record, 0)
	blockSizeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(blockSizeBytes, blockSize)
	blockSizeRecord := SetRec(0, 0, 1, uint64(len("block size")), uint64(len(blockSizeBytes)), "block size", blockSizeBytes)
	data = append(data, blockSizeRecord)
	blockManagerHeader.WriteBlock(data, fileName, 0)
}
