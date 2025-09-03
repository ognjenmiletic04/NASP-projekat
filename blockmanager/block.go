package blockmanager

type Block struct {
	records       []*Record
	blockNumber   uint64
	blockFilePath string
}

func (block *Block) GetRecords() []*Record {
	return block.records
}
func (block *Block) GetBlockNumber() uint64 {
	return block.blockNumber
}
func (block *Block) GetBlockFilePath() string {
	return block.blockFilePath
}

func (b *Block) SetRecords(records []*Record) {
	b.records = records
}

func (b *Block) SetBlockNumber(num uint64) {
	b.blockNumber = num
}

func (b *Block) SetBlockFilePath(path string) {
	b.blockFilePath = path
}
