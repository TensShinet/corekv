// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import "math"

// Filter is an encoded set of []byte keys.
type Filter []byte

// MayContainKey _
func (f Filter) MayContainKey(k []byte) bool {
	return f.MayContain(Hash(k))
}

func (f Filter) K() uint8 {
	return f[len(f) - 1]
}

// get 根据 hash 值得到 filter 中某一位的值
func (f Filter) get(h uint32) uint8 {
	x, y := posInFilter(h, len(f) - 1)
	return uint8((f[x] >> y) & 1)
}

// set 根据 hash 值将某一位置 1
func (f Filter) set(h uint32) {
	x, y := posInFilter(h, len(f) - 1)
	f[x] = f[x] | 1 << y
}

// MayContain returns whether the filter may contain given key. False positives
// are possible, where it returns true for keys not in the original set.
func (f Filter) MayContain(h uint32) bool {
	//Implement me here!!!
	//在这里实现判断一个数据是否在bloom过滤器中
	//思路大概是经过K个Hash函数计算，判读对应位置是否被标记为1
	delta, k := h >> 17 | h << 15, f.K()
	for j := uint8(0); j < k; j ++ {
		if f.get(h) == 0 {
			return false
		}
		h += delta
	}
	return true
}

// posInFilter 根据 hash 值计算此 hash 在 pos 的哪一个位置
// h 是 hash 值，filterLen 就是用byte数组中真正做做filter的长度
func posInFilter(h uint32, filterLen int) (x, y int) {
	nBits :=  uint32(filterLen * 8)
	bitPos := h % nBits
	return int(bitPos / 8), int(bitPos % 8)
}

// NewFilter returns a new Bloom filter that encodes a set of []byte keys with
// the given number of bits per key, approximately.
//
// A good bitsPerKey value is 10, which yields a filter with ~ 1% false
// positive rate.
func NewFilter(keys []uint32, bitsPerKey int) Filter {
	return appendFilter(keys, bitsPerKey)
}

// BloomBitsPerKey returns the bits per key required by bloomfilter based on
// the false positive rate.
func BloomBitsPerKey(numEntries int, fp float64) int {
	//Implement me here!!!
	//阅读bloom论文实现，并在这里编写公式
	//传入参数numEntries是bloom中存储的数据个数，fp是false positive假阳性率
	// 计算 m/n 根据：https://en.wikipedia.org/wiki/Bloom_filter
	return int(-1.44 * math.Log2(fp) + 1)
}

func appendFilter(keys []uint32, bitsPerKey int) Filter {
	//Implement me here!!!
	//在这里实现将多个Key值放入到bloom过滤器中
	// TODO：系统检查 bitsPerKey
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	keyLen := len(keys)
	k := uint8(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}

	if k > 30 {
		k = 30
	}

	nBits := bitsPerKey * keyLen

	// 如果 nBits 太小会有很高的 false positive
	if nBits < 64 {
		nBits = 64
	}

	// TODO：检查 nBits 的上界

	nBytes := (nBits + 7) / 8
	// 最后一位
	filter := Filter(make([]byte, nBytes + 1))


	// 向 filter 中放入所有的 key
	for _, h := range keys {
		delta := h >> 17 | h << 15
		for j := uint8(0); j < k; j ++ {
			filter.set(h)
			h += delta
		}
	}

	filter[nBytes] = k
	return filter
}



// Hash implements a hashing algorithm similar to the Murmur hash.
func Hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return h
}