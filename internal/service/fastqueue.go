package service

import (
	"sync"
	"sync/atomic"
)

var batchPool = sync.Pool{
	New: func() interface{} {
		return make([][]byte, 0, 64)
	},
}

type FastQueue struct {
	buffer [][]byte
	head   uint64
	_pad1  [7]uint64
	tail   uint64
	_pad2  [7]uint64
	mask   uint64
}

func NewFastQueue(size int) *FastQueue {
	if size&(size-1) != 0 {
		size = nextPowerOf2(size)
	}

	return &FastQueue{
		buffer: make([][]byte, size),
		mask:   uint64(size - 1),
	}
}

func (q *FastQueue) Push(data []byte) bool {
	for {
		head := atomic.LoadUint64(&q.head)
		next := head + 1

		if next&q.mask == atomic.LoadUint64(&q.tail)&q.mask {
			return false
		}

		if atomic.CompareAndSwapUint64(&q.head, head, next) {
			q.buffer[head&q.mask] = data
			return true
		}
	}
}

func (q *FastQueue) PushFront(data []byte) bool {
	tail := atomic.LoadUint64(&q.tail)
	head := atomic.LoadUint64(&q.head)

	if (head+1)&q.mask == tail&q.mask {
		return false
	}

	newTail := tail - 1
	q.buffer[newTail&q.mask] = data
	atomic.StoreUint64(&q.tail, newTail)
	return true
}

func (q *FastQueue) Pop() ([]byte, bool) {
	tail := atomic.LoadUint64(&q.tail)
	head := atomic.LoadUint64(&q.head)

	if tail == head {
		return nil, false
	}

	data := q.buffer[tail&q.mask]
	atomic.StoreUint64(&q.tail, tail+1)
	return data, true
}

func (q *FastQueue) PopBatch(maxCount int) [][]byte {
	if maxCount <= 0 {
		return nil
	}

	tail := atomic.LoadUint64(&q.tail)
	head := atomic.LoadUint64(&q.head)

	available := int(head - tail)
	if available <= 0 {
		return nil
	}

	if available > maxCount {
		available = maxCount
	}

	batch := batchPool.Get().([][]byte)[:0]
	if cap(batch) < available {
		batchPool.Put(batch)
		batch = make([][]byte, 0, available)
	}

	for i := 0; i < available; i++ {
		batch = append(batch, q.buffer[(tail+uint64(i))&q.mask])
	}

	atomic.StoreUint64(&q.tail, tail+uint64(available))
	return batch
}

func (q *FastQueue) ReleaseBatch(batch [][]byte) {
	if batch != nil && cap(batch) <= 256 {
		batchPool.Put(batch)
	}
}

func (q *FastQueue) Size() int {
	head := atomic.LoadUint64(&q.head)
	tail := atomic.LoadUint64(&q.tail)
	return int(head - tail)
}

func nextPowerOf2(n int) int {
	if n <= 1 {
		return 2
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}
