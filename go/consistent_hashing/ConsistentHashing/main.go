package ConsistentHashing

import (
	"fmt"
	"math"
	"sort"

	"github.com/dangermike/hashing/go/consistent_hashing/ObjectHasher"
)

// BucketPlace is the location of a bucket on the ring
type BucketPlace struct {
	Place  uint64
	Bucket int
}

// ConsistentHashRing is just a collection of BucketPlace(s), sorted by place
type ConsistentHashRing struct {
	Buckets  []BucketPlace
	Replicas int
}

// New makes a new ring given a set of buckets and replicas.
// buckets should be a positive, non-zero number
// replicas will default to 200 if less than or equal to zero
func New(buckets int, replicas int) *ConsistentHashRing {
	if replicas <= 0 {
		replicas = buckets * buckets
	}
	ring := make([]BucketPlace, buckets*replicas, buckets*replicas)
	for b := 0; b < buckets; b++ {
		for r := 0; r < replicas; r++ {
			place := ObjectHasher.PlaceUInt64N(uint64(b), r+1)
			ring[(b*replicas)+r] = BucketPlace{place, b}
		}
	}
	sort.Slice(ring, func(i, j int) bool {
		return ring[i].Place < ring[j].Place
	})
	return &ConsistentHashRing{ring, replicas}
}

// MapBucket will return the correct bucket for the provided hash value
// In consistent hashing, this is the next bucket number on the ring for a
// given location
func (ring *ConsistentHashRing) MapBucket(location uint64) int {
	rr := ring.Buckets
	i := sort.Search(
		len(rr),
		func(i int) bool { return rr[i].Place >= location },
	)
	if i >= len(rr) {
		return rr[0].Bucket
	}
	return rr[i].Bucket
}

// ExpectedMoveRate returns the rate (0-1) at which members are expected to move
func (ring *ConsistentHashRing) ExpectedMoveRate(otherSize int) float64 {
	buckets := 1 + (len(ring.Buckets) / ring.Replicas)
	otherSize++
	return math.Abs(float64(otherSize-buckets)) / float64(buckets)
}

// Name tells you who we are
func (ring *ConsistentHashRing) Name() string {
	return fmt.Sprintf("ConsistentHashRing[%d, %d]", len(ring.Buckets)/ring.Replicas, ring.Replicas)
}
