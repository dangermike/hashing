package MaglevHashing

import (
	"fmt"
	"math"
	"sort"

	"github.com/dangermike/hashing/go/consistent_hashing/ObjectHasher"
)

// bucketPrimes is a selection of prime numbers we can use as the bucket count.
var bucketPrimes = []int{
	2, 5, 11, 23, 47, 89, 163, 331, 641, 1277, 1453, 2003, 2549, 3001, 3557, 4001,
	4507, 5003, 5501, 6007, 6521, 7001, 7507, 8009, 8501, 9001, 9511, 10501,
	11003, 11503, 12007, 12503, 12511, 13001, 13513, 14009, 14503, 15013, 15511,
	16001, 16519, 17011, 17509, 18013, 18503, 19001, 19501, 20011, 20507, 21001,
	21503, 22003, 22501, 23003, 23509, 24001, 24509, 25013, 25523, 26003, 26501,
	27011, 27509, 28001, 28513, 29009, 29501, 30011, 30509, 31013, 31511, 32003,
	32503, 33013, 33503, 34019, 34501, 35023, 35507, 36007, 36523, 37003, 37501,
	38011, 38501, 39019, 39503, 40009, 40507, 41011, 41507, 42013, 42509, 43003,
	43517, 44017, 45007, 45503, 46021, 46507, 47017, 47501, 48017, 48523, 49003,
	49523, 50021, 50503, 51001, 51503, 52009, 52501, 53003, 53503, 54001, 54503,
	55001, 55501, 56003, 56501, 57037, 57503, 58013, 58511, 59009, 59509, 60013,
	60509, 61001, 61507, 62003, 62501, 63029, 63521, 64007, 64513, 65003, 65519,
	66029, 66509, 67003, 67511, 68023, 68501, 69001, 69539, 70001, 70501, 71011,
	71503, 72019, 72503, 73009, 73517, 74017, 74507, 75011, 75503, 76001, 76507,
	77003, 77509, 78007, 78509, 79031, 79531, 80021, 80513, 81001, 81509, 82003,
	82507, 83003, 83537, 84011, 84503, 85009, 85513, 86011, 86501, 87011, 87509,
	88001, 88513, 89003, 89501, 90001, 90511, 91009, 91513, 92003, 92503, 93001,
	93503, 94007, 94513, 95003, 95507, 96001, 96517, 97001, 97501, 98009, 98507,
	99013, 99523,
}

// MaglevHasher distributes buckets into a lookup table
type MaglevHasher struct {
	Buckets     uint64
	lookupTable []int16
}

// New creates a new MaglevHasher
// buckets is the number of buckets to select from. sizeClass is the approximate
// maximum number of buckets. Changing this number will completely reshuffle the
// lookup table, which is bad if the buckets are supposed to represent machines
// that could fail or something similar. This implementation only supports an
// integer bucket count, but the algorithm supports removing arbitrary buckets
// in the middle.
func New(buckets int, sizeClass int) *MaglevHasher {
	if sizeClass < 0 {
		sizeClass = buckets
	}
	bix := sort.SearchInts(bucketPrimes, 100*sizeClass)
	if bix >= len(bucketPrimes) {
		bix = len(bucketPrimes) - 1
	}
	tableSize := bucketPrimes[bix]
	table := make([]int16, tableSize, tableSize)

	// Initialize the table to -1, indicating that the slot is empty
	for ix := 0; ix < tableSize; ix++ {
		table[ix] = int16(-1)
	}

	// each bucket gets an offset and a skip, which is used when choosing each
	// bucket's preferred slot in the table
	bucketGroup := make([][2]int, buckets, buckets)
	for ix := 0; ix < buckets; ix++ {
		offset := ObjectHasher.PlaceUInt64N(uint64(ix), 1) % uint64(tableSize)
		skip := ObjectHasher.PlaceUInt64N(uint64(ix), 1) % uint64((tableSize-1)+1)
		bucketGroup[ix] = [2]int{int(offset), int(skip)}
	}

	// go through each bucket, letting it take its first preferred, unoccupied slot
	for ix := 0; ix < tableSize; ix++ {
		bucket := ix % buckets
		offset := bucketGroup[bucket][0]
		skip := bucketGroup[bucket][1]
		for table[offset] >= 0 {
			offset = (offset + skip) % tableSize
		}
		table[offset] = int16(bucket)
		bucketGroup[bucket][0] = (offset + skip) % tableSize
	}
	return &MaglevHasher{uint64(buckets), table}
}

// MapBucket returns the target bucket for a given object hash
func (mh *MaglevHasher) MapBucket(location uint64) int {
	return int(mh.lookupTable[(location % uint64(len(mh.lookupTable)))])
}

// ExpectedMoveRate is the rate we would expect random elements to move given
// the current size of the MaglevHasher and an alternative size.
func (mh *MaglevHasher) ExpectedMoveRate(otherSize int) float64 {
	otherSize++
	buckets := int(mh.Buckets + 1)
	return math.Abs(float64(otherSize-buckets)) / float64(buckets)
}

// Name tells yo who we are
func (mh *MaglevHasher) Name() string {
	return fmt.Sprintf("MaglevHasher[%d]", mh.Buckets)
}
