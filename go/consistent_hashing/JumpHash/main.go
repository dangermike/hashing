package JumpHash

import (
	"fmt"
	"math"
)

// JumpHash is a random number generator acting like a consistent hash
// function and doing a good job of it. https://arxiv.org/pdf/1406.2294.pdf
type JumpHash struct {
	buckets uint64
}

// New makes a new JumpHash
func New(buckets int) *JumpHash {
	return &JumpHash{uint64(buckets)}
}

// MapBucket returns the target bucket for a given object
func (jh *JumpHash) MapBucket(location uint64) int {
	b := uint64(1)
	j := uint64(0)
	for j < jh.buckets {
		b = j
		location = location*uint64(2862933555777941757) + 1
		j = uint64(float64(b+1) * (float64(1<<31) / float64((location>>33)+1)))
	}
	return int(b)
}

// ExpectedMoveRate returns the rate (0-1) at which members are expected to move
func (jh *JumpHash) ExpectedMoveRate(otherSize int) float64 {
	otherSize++
	buckets := int(jh.buckets + 1)
	return math.Abs(float64(otherSize-buckets)) / float64(buckets)
}

// Name tells you who we are
func (jh *JumpHash) Name() string {
	return fmt.Sprintf("JumpHash[%d]", jh.buckets)
}

/*
int32_t JumpConsistentHash(uint64_t key, int32_t num_buckets) {
  int64_t b = ­1, j = 0;
  while (j < num_buckets) {
    b = j;
    key = key * 2862933555777941757ULL + 1;
    j = (b + 1) * (double(1LL << 31) / double((key >> 33) + 1));
  }
  return b;
}
*/
