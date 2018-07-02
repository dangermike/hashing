package ObjectHasher

import (
	"encoding/binary"

	"github.com/OneOfOne/xxhash"
)

func PlaceUInt64N(o uint64, ix int) uint64 {
	if ix > 0 {
		b := make([]byte, 8)
		for ; ix >= 1; ix-- {
			binary.LittleEndian.PutUint64(b, o)
			o = xxhash.Checksum64(b)
		}
	}
	return o
}

func PlaceStringN(s string, ix int) uint64 {
	return PlaceUInt64N(xxhash.ChecksumString64(s), ix)
}

func PlaceString(s string) uint64 {

	return xxhash.ChecksumString64(s)
}
