package ModHashing

import (
	"fmt"
	"math"
)

// ModHasher uses a simple mod of the object's hash to determine the bucket
type ModHasher struct {
	Buckets uint64
}

// New creates a new ModHasher
func New(buckets int) *ModHasher {
	return &ModHasher{uint64(buckets)}
}

// MapBucket returns the target bucket for a given object hash
func (mh *ModHasher) MapBucket(location uint64) int {
	return int(location % mh.Buckets)
}

// ExpectedMoveRate is the rate we would expect random elements to move given
// the current size of the ModHasher and an alternative size.
func (mh *ModHasher) ExpectedMoveRate(otherSize int) float64 {
	// 1 - (gcd(mh.Buckets, otherSize)/max(mh.Buckets, otherSize))
	return 1.0 - (float64(gcd(mh.Buckets, uint64(otherSize))) / math.Max(float64(mh.Buckets), float64(otherSize)))
}

func gcd(x uint64, y uint64) uint64 {
	mf := uint64(math.Floor(math.Sqrt(math.Max(float64(x), float64(y)))))
	_gcd := uint64(1)
	for i := mf; i > 1; i-- {
		if x%i == 0 && y%i == 0 {
			_gcd *= i
			x /= i
			y /= i
		}
	}
	return _gcd
}

// Name tells yo who we are
func (mh *ModHasher) Name() string {
	return fmt.Sprintf("ModHash[%d]", mh.Buckets)
}
