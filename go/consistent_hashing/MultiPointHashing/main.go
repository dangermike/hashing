package MultiPointHashing

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/OneOfOne/xxhash"
	"github.com/dangermike/hashing/go/consistent_hashing/ObjectHasher"
)

// BucketPlace is the location of a bucket on the ring
type BucketPlace struct {
	Place  uint64
	Bucket int
}

// MultiPointHashRing is just a collection of BucketPlace(s), sorted by place
type MultiPointHashRing struct {
	Buckets []BucketPlace
	Tries   uint
}

// New makes a new ring given a set of buckets
func New(buckets int, tries uint) *MultiPointHashRing {
	if tries <= 0 {
		tries = 21
	}
	ring := make([]BucketPlace, buckets, buckets)
	for b := 0; b < buckets; b++ {
		place := ObjectHasher.PlaceUInt64N(uint64(b), 1)
		ring[b] = BucketPlace{place, b}
	}
	sort.Slice(ring, func(i, j int) bool {
		return ring[i].Place < ring[j].Place
	})
	return &MultiPointHashRing{ring, tries}
}

// MapBucket will return the correct bucket for the provided hash value
// In consistent hashing, this is the next bucket number on the ring for a
// given location
func (ring *MultiPointHashRing) MapBucket(location uint64) int {
	rr := ring.Buckets
	var bestDistance uint64 = math.MaxUint64
	bestBucket := -1

	b := make([]byte, 8)
	h := xxhash.New64()
	for i := uint(0); i < ring.Tries; i++ {
		ix := sort.Search(
			len(rr),
			func(ix int) bool { return rr[ix].Place >= location },
		)
		if ix >= len(rr) {
			ix = 0
		}
		distance := rr[ix].Place - location
		if distance < bestDistance {
			bestDistance = distance
			bestBucket = rr[ix].Bucket
		}
		binary.LittleEndian.PutUint64(b, location)
		h.Write(b)
		location = h.Sum64()
	}
	return bestBucket
}

// ExpectedMoveRate returns the rate (0-1) at which members are expected to move
func (ring *MultiPointHashRing) ExpectedMoveRate(otherSize int) float64 {
	buckets := 1 + len(ring.Buckets)
	otherSize++
	return math.Abs(float64(otherSize-buckets)) / float64(buckets)
}

// Name tells you who we are
func (ring *MultiPointHashRing) Name() string {
	return fmt.Sprintf("MultiPointHashRing[%d, %d]", len(ring.Buckets), ring.Tries)
}
