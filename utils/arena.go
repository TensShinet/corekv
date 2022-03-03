package utils

import (
	"github.com/pkg/errors"
	"log"
	"sync/atomic"
	"unsafe"
)

type Arena struct {
	n   uint32 //offset
	buf []byte
}

const MaxNodeSize = uint32(unsafe.Sizeof(Element{}))
// MaxArenaSize Arena 最大的 size 是 200M，有很多好处，可以防止溢出。
// 不需要在代码里添加很多 corner case 的检查。
const MaxArenaSize = 100 << 20

const offsetSize = uint32(unsafe.Sizeof(uint32(0)))
const nodeAlign = uint32(unsafe.Sizeof(uint64(0))) - 1

func newArena(n int64) *Arena {
	out := &Arena{
		n:   1,
		buf: make([]byte, n),
	}
	return out
}

// allocate 在 arena 中分配指定大小的内存空间，返回 offset
// 1. 如果空间够（放完这个 sz 以后还能放下下一个 MaxNodeSize），直接返回当前 offset
// 2. 如果空间不够，二倍增长
// 3. 检查增长，如果增长大于 1 << (10 << 20)（10M），增长 = (10 << 20)
// 4. 检查增长，如果增长完大于 MaxArenaSize，增长 = MaxArenaSize - 当前 offset
// 5. 检查增长，如果增长小于 sz，增长 = sz
// 6. 检查增长，如果增长以后大于 MaxArenaSize panic
// 7. 创建新的 buf，将旧 buf copy 到新的 buf 中
// 8. 返回 offset
func (s *Arena) allocate(sz uint32) uint32 {
	//implement me here！！！
	if sz > MaxArenaSize {
		// TODO: return error
		panic("sz is too big")
	}

	currentSize := uint32(len(s.buf))
	newOffset := sz + s.n

	if newOffset > MaxArenaSize {
		// 超过预期估计最大 size
		// TODO: return error
		panic("sz is too big")
	}

	if currentSize - MaxNodeSize < newOffset {
		// 空间不够需要增长
		growth := uint32(len(s.buf))

		if growth > 10 << 20 {
			growth = 10 << 20
		}

		if growth < sz {
			growth = sz
		}

		if growth + currentSize > MaxArenaSize {
			growth = MaxArenaSize - currentSize
			if growth < sz {
				// 超过预期估计最大 size
				// TODO: return error
				panic("sz is too big")
			}
		}

		newBuf := make([]byte, currentSize + growth)
		AssertTrue(len(s.buf) == copy(newBuf, s.buf))
		s.buf = newBuf
	}

	s.n = newOffset

	return s.n - sz
}

// putNode 在arena里开辟一块空间，用以存放sl中的节点
// 返回值为在arena中的offset
// 1. 计算这个 node 需要多少空间：MaxNodeSize - offsetSize * (defaultMaxLevel - height)
// 2. 计算在内存对齐的情况下，需要多少空间
// 3. 分配空间
// 4. 计算返回 offset
func (s *Arena) putNode(height int) uint32 {
	//implement me here！！！
	// 这里的 node 要保存 value 、key 和 next 指针值
	// 所以要计算清楚需要申请多大的内存空间
	nodeSize := MaxNodeSize - offsetSize * (defaultMaxLevel - uint32(height))
	alignedNodeSize := nodeSize + nodeAlign
	offset := s.allocate(alignedNodeSize)
	return (offset + nodeAlign) & (^nodeAlign)
}

// putVal 将 v encode 以后放入 arena 中，返回 offset
// 返回的指针值应被存储在 Node 节点中
// 1. 将 value 编码，得到 value 的大小
// 2. 分配空间
// 3. 把编码后的 value 放入
// 4. 返回 value 的起始地址
func (s *Arena) putVal(v ValueStruct) uint32 {
	//implement me here！！！
	vs := v.EncodedSize()
	offset := s.allocate(vs)
	v.EncodeValue(s.buf[offset:])
	return offset
}

// putKey 将 Key 值存储到 arena 当中
// 并且将指针返回，返回的指针值应被存储在 Node 节点中
// 1. 得到 key 的所需要的空间
// 2. 分配空间
// 3. 将 key 放入
func (s *Arena) putKey(key []byte) uint32 {
	//implement me here！！！
	keySize := len(key)
	offset := s.allocate(uint32(keySize))
	AssertTrue(len(key) == copy(s.buf[offset:], key))
	return offset
}

func (s *Arena) getElement(offset uint32) *Element {
	if offset == 0 {
		return nil
	}

	return (*Element)(unsafe.Pointer(&s.buf[offset]))
}

func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

func (s *Arena) getVal(offset uint32, size uint32) (v ValueStruct) {
	v.DecodeValue(s.buf[offset : offset+size])
	return
}

//用element在内存中的地址 - arena首字节的内存地址，得到在arena中的偏移量
func (s *Arena) getElementOffset(nd *Element) uint32 {
	//implement me here！！！
	//获取某个节点，在 arena 当中的偏移量
	if nd == nil {
		return 0
	}

	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

func (e *Element) getNextOffset(h int) uint32 {
	//implement me here！！！
	// 这个方法用来计算节点在h 层数下的 next 节点
	return atomic.LoadUint32(&e.levels[h])
}

func (s *Arena) Size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}