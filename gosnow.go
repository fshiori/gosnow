/*
github.com/twitter/snowflake in golang
*/

package gosnow

import (
	"fmt"
	"hash/crc32"
	"math/rand"
	"net"
	"sync"
	"time"
)

const (
	nano = 1000 * 1000
)

const (
	WorkerIdBits = 10              // worker id
	MaxWorkerId  = -1 ^ (-1 << 10) // worker id mask
	SequenceBits = 12              // sequence
	MaxSequence  = -1 ^ (-1 << 12) //sequence mask
)

var (
	Since   int64 = time.Date(2012, 1, 0, 0, 0, 0, 0, time.UTC).UnixNano() / nano
	Default *SnowFlake
)

func init() {
	var err error
	Default, err = NewSnowFlake(DefaultWorkId())
	if err != nil {
		panic(err)
	}
}

type SnowFlake struct {
	lastTimestamp uint64
	workerId      uint32
	sequence      uint32
	lock          sync.Mutex
}

func (sf *SnowFlake) uint64(ts uint64) uint64 {
	return (ts << (WorkerIdBits + SequenceBits)) |
		(uint64(sf.workerId) << SequenceBits) |
		(uint64(sf.sequence))
}

func (sf *SnowFlake) Next() uint64 {
	ts := timestamp()
	var delay uint64
	sf.lock.Lock()
	if ts == sf.lastTimestamp {
		sf.sequence = (sf.sequence + 1) & MaxSequence
		if sf.sequence == 0 {
			delay = 1
		}
	} else if ts < sf.lastTimestamp {
		delay = (sf.lastTimestamp - ts)

	} else {
		sf.sequence = 0
	}

	id := sf.uint64(ts)
	sf.lastTimestamp = ts

	sf.lock.Unlock()

	if delay > 0 {
		time.Sleep(time.Duration(delay) * time.Millisecond)
		return sf.Next()
	}
	return id
}

func NewSnowFlake(workerId uint32) (*SnowFlake, error) {
	if workerId < 0 || workerId > MaxWorkerId {
		return nil, fmt.Errorf("Worker id %v is invalid", workerId)
	}
	return &SnowFlake{workerId: workerId}, nil
}

func timestamp() uint64 {
	return uint64(time.Now().UnixNano()/nano - Since)
}

func DefaultWorkId() uint32 {
	var id uint32
	ift, err := net.Interfaces()
	if err != nil {
		rand.Seed(time.Now().UnixNano())
		id = rand.Uint32() % MaxWorkerId
	} else {
		h := crc32.NewIEEE()
		for _, value := range ift {
			h.Write(value.HardwareAddr)
		}
		id = h.Sum32() % MaxWorkerId
	}
	return id & MaxWorkerId
}
