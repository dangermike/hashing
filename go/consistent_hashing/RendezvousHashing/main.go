package RendezvousHashing

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/OneOfOne/xxhash"
)

// RendezvousHashGroup maintains uniformity and least-moves by hashing the
// incoming value with the bucket and taking the bucket with the highest
// combined value
type RendezvousHashGroup struct {
	Buckets uint64
}

// New makes a new rendezvous hash group.
// buckets should be a positive, non-zero number
func New(buckets int) *RendezvousHashGroup {
	return &RendezvousHashGroup{uint64(buckets)}
}

// MapBucket will return the correct bucket for the provided hash value
func (rhg *RendezvousHashGroup) MapBucket(location uint64) int {
	maxIx := uint64(0)
	maxHash := uint64(0)

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, location)

	for ix := uint64(0); ix < rhg.Buckets; ix++ {
		h := xxhash.NewS64(ix)
		h.Write(b)
		hv := h.Sum64()
		if hv > maxHash {
			maxIx = ix
			maxHash = hv
		}
	}
	return int(maxIx)
}

// ExpectedMoveRate returns the rate (0-1) at which members are expected to move
func (rhg *RendezvousHashGroup) ExpectedMoveRate(otherSize int) float64 {
	otherSize++
	buckets := int(rhg.Buckets + 1)
	return math.Abs(float64(otherSize-buckets)) / float64(buckets)
}

// Name tells you who we are
func (rhg *RendezvousHashGroup) Name() string {
	return fmt.Sprintf("RendezvousHash[%d]", rhg.Buckets)
}
