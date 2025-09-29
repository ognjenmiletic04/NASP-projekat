package main

import (
	"encoding/json"
	"os"
)

type Config struct {
	BlockSize     uint64 `json:"blockSize"`
	MemCapacity   int    `json:"memCapacity"`
	SummaryStep   int    `json:"summaryStep"`
	CacheCapacity int    `json:"cacheCapacity"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// prvo postavi default vrednosti
	cfg := &Config{
		BlockSize:     4096, // 4KB
		MemCapacity:   2,
		SummaryStep:   2,
		CacheCapacity: 5,
	}

	// zatim prepiši vrednosti iz JSON-a (ako postoje)
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	// validacija – ako je neko uneo glupost (npr. 0 ili negativno)
	if cfg.BlockSize <= 0 {
		cfg.BlockSize = 4096
	}
	if cfg.MemCapacity <= 0 {
		cfg.MemCapacity = 2
	}
	if cfg.SummaryStep <= 0 {
		cfg.SummaryStep = 2
	}
	if cfg.CacheCapacity <= 0 {
		cfg.CacheCapacity = 5
	}

	return cfg, nil
}
