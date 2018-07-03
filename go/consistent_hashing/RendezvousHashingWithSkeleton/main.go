package RendezvousHashingWithSkeleton

import (
	"encoding/binary"
	"math"

	"github.com/OneOfOne/xxhash"
)

type member interface {
	mapBucket(locationBytes []byte) int
}

type innerGroup struct {
	children []member
}

func (ig *innerGroup) mapBucket(locationBytes []byte) int {
	maxIx := -1
	maxHashVal := uint64(0)

	for ix := 0; ix < len(ig.children); ix++ {
		h := xxhash.NewS64(uint64(ix))
		h.Write(locationBytes)
		hv := h.Sum64()
		if hv > maxHashVal {
			maxIx = ix
			maxHashVal = hv
		}
	}
	return ig.children[maxIx].mapBucket(locationBytes)
}

type cluster struct {
	minBucket int
	maxBucket int
}

func (c cluster) mapBucket(locationBytes []byte) int {
	maxIx := -1
	maxHashVal := uint64(0)

	for ix := c.minBucket; ix <= c.maxBucket; ix++ {
		h := xxhash.NewS64(uint64(ix))
		h.Write(locationBytes)
		hv := h.Sum64()
		if hv > maxHashVal {
			maxIx = ix
			maxHashVal = hv
		}
	}
	return maxIx
}

// RendezvousHashGroup maintains uniformity and least-moves by hashing the
// incoming value with the bucket and taking the bucket with the highest
// combined value
type RendezvousHashGroup struct {
	children []member
	Buckets  int
	M        int
	F        int
}

// New makes a new rendezvous hash group.
// buckets should be a positive, non-zero number
// m is the cluster size -- max number of buckets in a leaf node
// f is the fanout -- max size of an inner node
func New(buckets int, m int, f int) *RendezvousHashGroup {
	clusterCnt := 1 + (buckets-1)/m
	members := make([]member, clusterCnt, clusterCnt)
	for ix := 0; ix*m < buckets; ix++ {
		firstB := m * ix
		lastB := firstB + m - 1
		if lastB >= buckets {
			lastB = buckets - 1
		}
		members[ix] = cluster{firstB, lastB}
	}

	for len(members) > f {
		newCnt := 1 + (len(members)-1)/f
		newMembers := make([]member, newCnt, newCnt)
		for ix := 0; len(members) > 0; ix++ {
			if f >= len(members) {
				newMembers[ix] = &innerGroup{members}
				members = members[0:0]
			} else {
				newMembers[ix] = &innerGroup{members[:f]}
				members = members[f:]
			}
		}

		members = newMembers
	}

	return &RendezvousHashGroup{members, buckets, m, f}
}

// MapBucket will return the correct bucket for the provided hash value
func (rhg *RendezvousHashGroup) MapBucket(location uint64) int {
	maxIx := 0
	maxHash := uint64(0)

	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, location)

	for ix := 0; ix < len(rhg.children); ix++ {
		h := xxhash.NewS64(uint64(ix))
		h.Write(b)
		hv := h.Sum64()
		if hv > maxHash {
			maxIx = ix
			maxHash = hv
		}
	}
	return rhg.children[maxIx].mapBucket(b)
}

// ExpectedMoveRate returns the rate (0-1) at which members are expected to move
func (rhg *RendezvousHashGroup) ExpectedMoveRate(otherSize int) float64 {
	otherSize++
	buckets := int(rhg.Buckets + 1)
	return math.Abs(float64(otherSize-buckets)) / float64(buckets)
}
