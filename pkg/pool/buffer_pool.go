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

	ResponseBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 512)
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

func GetResponseBuffer() []byte {
	return ResponseBufferPool.Get().([]byte)[:0]
}

func PutResponseBuffer(buf []byte) {
	if cap(buf) > 8192 {
		return
	}
	ResponseBufferPool.Put(buf)
}