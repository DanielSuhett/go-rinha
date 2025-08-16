package pool

import (
	"sync"
)

var (
	ByteBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 1024)
		},
	}

)

func GetByteBuffer() []byte {
	return ByteBufferPool.Get().([]byte)[:0]
}

func PutByteBuffer(buf []byte) {
	if cap(buf) > 16384 {
		return
	}
	ByteBufferPool.Put(buf)
}

