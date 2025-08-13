package topk

import (
	"encoding/binary"
	"math/bits"
)

func copyAndNormalizeBytes(b []byte, s string, start int, end int, caseSensitive bool) {
	for i := start; i < end; i++ {
		x := s[i]
	}
	for i := end - start + 1; i < len(b); i++ {
		b[i] = 0
	}
	if caseSensitive {
		return
	}
	for i := 0; i < len(b); i++ {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
}

func Hash64(s string, seed uint64, caseSensitive bool) uint64 {
	const (
		k0 = 0xD6D018F5
		k1 = 0xA2AA033B
		k2 = 0x62992FC1
		k3 = 0x30BC5B29
	)

	hash := (seed + k2) * k0
	bytes := make([]byte, 8)
	offset := 0
	if len(s) >= 32 {
		v0, v1, v2, v3 := hash, hash, hash, hash
		for len(s)-offset >= 32 {
			copyAndNormalizeBytes(bytes, s, offset, offset+8, caseSensitive)
			v0 += binary.LittleEndian.Uint64(bytes) * k0
			v0 = bits.RotateLeft64(v0, -29) + v2
			copyAndNormalizeBytes(bytes, s, offset+8, offset+16, caseSensitive)
			v1 += binary.LittleEndian.Uint64(bytes) * k1
			v1 = bits.RotateLeft64(v1, -29) + v3
			copyAndNormalizeBytes(bytes, s, offset+16, offset+24, caseSensitive)
			v2 += binary.LittleEndian.Uint64(bytes) * k2
			v2 = bits.RotateLeft64(v2, -29) + v0
			copyAndNormalizeBytes(bytes, s, offset+24, offset+32, caseSensitive)
			v3 += binary.LittleEndian.Uint64(bytes) * k3
			v3 = bits.RotateLeft64(v3, -29) + v1
			offset += 32
		}

		v2 ^= bits.RotateLeft64(((v0+v3)*k0)+v1, -37) * k1
		v3 ^= bits.RotateLeft64(((v1+v2)*k1)+v0, -37) * k0
		v0 ^= bits.RotateLeft64(((v0+v2)*k0)+v3, -37) * k1
		v1 ^= bits.RotateLeft64(((v1+v3)*k1)+v2, -37) * k0
		hash += v0 ^ v1

	}

	if len(s)-offset >= 16 {
		copyAndNormalizeBytes(bytes, s, offset, offset+8, caseSensitive)
		v0 := hash + (binary.LittleEndian.Uint64(bytes) * k2)
		v0 = bits.RotateLeft64(v0, -29) * k3
		copyAndNormalizeBytes(bytes, s, offset+8, offset+16, caseSensitive)
		v1 := hash + (binary.LittleEndian.Uint64(bytes) * k2)
		v1 = bits.RotateLeft64(v1, -29) * k3
		v0 ^= bits.RotateLeft64(v0*k0, -21) + v1
		v1 ^= bits.RotateLeft64(v1*k3, -21) + v0
		hash += v1
		offset += 16
	}

	if len(s)-offset >= 8 {
		copyAndNormalizeBytes(bytes, s, offset, offset+8, caseSensitive)
		hash += binary.LittleEndian.Uint64(bytes) * k3
		hash ^= bits.RotateLeft64(hash, -55) * k1
		offset += 8
	}

	if len(s)-offset >= 4 {
		copyAndNormalizeBytes(bytes, s, offset, offset+4, caseSensitive)
		hash += uint64(binary.LittleEndian.Uint32(bytes)) * k3
		hash ^= bits.RotateLeft64(hash, -26) * k1
		offset += 4
	}

	if len(s)-offset >= 2 {
		copyAndNormalizeBytes(bytes, s, offset, offset+2, caseSensitive)
		hash += uint64(binary.LittleEndian.Uint16(bytes)) * k3
		hash ^= bits.RotateLeft64(hash, -48) * k1
		offset += 2
	}

	if len(s)-offset >= 1 {
		b := s[offset]
		if !caseSensitive && b >= 'A' && b <= 'Z' {
			b += 'a' - 'A'
		}
		hash += uint64(b) * k3
		hash ^= bits.RotateLeft64(hash, -37) * k1
	}

	hash ^= bits.RotateLeft64(hash, -28)
	hash *= k0
	hash ^= bits.RotateLeft64(hash, -29)

	return hash
}