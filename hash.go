package topk

import (
	"encoding/binary"
	"math/bits"
	"unicode"
	"unicode/utf8"
)

func Hash64(s string, seed uint64, caseSensitive bool) uint64 {
	const (
		k0 = 0xD6D018F5
		k1 = 0xA2AA033B
		k2 = 0x62992FC1
		k3 = 0x30BC5B29
	)

	bytes := make([]byte, 0, 35)
	runeBytes := make([]byte, utf8.UTFMax)
	hash := (seed + k2) * k0
	v0, v1, v2, v3 := hash, hash, hash, hash

	for _, c := range s {
		if unicode.IsLetter(c) && !caseSensitive {
			c = unicode.ToLower(c)
		}
		n := utf8.EncodeRune(runeBytes, c)
		bytes = append(bytes, runeBytes[:n]...)
		if len(bytes) >= 32 {
			v0 += binary.LittleEndian.Uint64(bytes[:8]) * k0
			v0 = bits.RotateLeft64(v0, -29) + v2
			v1 += binary.LittleEndian.Uint64(bytes[8:16]) * k1
			v1 = bits.RotateLeft64(v1, -29) + v3
			v2 += binary.LittleEndian.Uint64(bytes[16:24]) * k2
			v2 = bits.RotateLeft64(v2, -29) + v0
			v3 += binary.LittleEndian.Uint64(bytes[24:32]) * k3
			v3 = bits.RotateLeft64(v3, -29) + v1

			v2 ^= bits.RotateLeft64(((v0+v3)*k0)+v1, -37) * k1
			v3 ^= bits.RotateLeft64(((v1+v2)*k1)+v0, -37) * k0
			v0 ^= bits.RotateLeft64(((v0+v2)*k0)+v3, -37) * k1
			v1 ^= bits.RotateLeft64(((v1+v3)*k1)+v2, -37) * k0
			hash += v0 ^ v1
			// clear first 32 bytes and shift right by 32
			copy(bytes[:], bytes[32:])
			bytes = bytes[:len(bytes)-32]
		}
	}

	if len(bytes) >= 16 {
		v0 := hash + (binary.LittleEndian.Uint64(bytes[:8]) * k2)
		v0 = bits.RotateLeft64(v0, -29) * k3
		v1 := hash + (binary.LittleEndian.Uint64(bytes[8:16]) * k2)
		v1 = bits.RotateLeft64(v1, -29) * k3
		v0 ^= bits.RotateLeft64(v0*k0, -21) + v1
		v1 ^= bits.RotateLeft64(v1*k3, -21) + v0
		hash += v1
		bytes = bytes[16:]
	}

	if len(bytes) >= 8 {
		hash += binary.LittleEndian.Uint64(bytes[:8]) * k3
		bytes = bytes[8:]
		hash ^= bits.RotateLeft64(hash, -55) * k1
	}

	if len(bytes) >= 4 {
		hash += uint64(binary.LittleEndian.Uint32(bytes[:4])) * k3
		hash ^= bits.RotateLeft64(hash, -26) * k1
		bytes = bytes[4:]
	}

	if len(bytes) >= 2 {
		hash += uint64(binary.LittleEndian.Uint16(bytes[:2])) * k3
		bytes = bytes[2:]
		hash ^= bits.RotateLeft64(hash, -48) * k1
	}

	if len(bytes) >= 1 {
		hash += uint64(bytes[0]) * k3
		hash ^= bits.RotateLeft64(hash, -37) * k1
	}

	hash ^= bits.RotateLeft64(hash, -28)
	hash *= k0
	hash ^= bits.RotateLeft64(hash, -29)

	return hash
}
