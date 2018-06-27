package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/OneOfOne/xxhash"
)

// var d0 = []string{
// 	"spiffy", "amusing", "weigh", "milk", "groan", "utter", "low", "abusive", "fill", "spark",
// 	"important", "joke", "snail", "crib", "chalk", "group", "pull", "impress", "capable", "design",
// 	"fry", "authority", "exclusive", "nutritious", "robin", "book", "upbeat", "smoke", "oval",
// 	"sparkling", "available", "domineering", "treatment", "friends", "alert", "occur", "level",
// 	"old-fashioned", "unadvised", "crabby", "languid", "radiate", "wine", "pest", "behavior",
// 	"drown", "eggs", "tasteless", "check", "peace",
// }

var d0 = []string{
	"impress", "road", "furniture", "geese", "screw", "phobic", "guard", "ghost", "yam",
	"boundary", "floor", "careless", "dashing", "umbrella", "root", "rhyme", "ahead", "kiss",
	"territory", "part", "big", "spiders", "quiet", "unequal", "damaging", "permit", "camera",
	"improve", "gifted", "interest", "habitual", "unit", "step", "sisters", "squeak", "race",
	"skip", "weather", "tasteful", "victorious", "jagged", "preserve", "plants", "queen",
	"fearless", "caption", "belief", "uptight", "windy", "paper", "truculent", "hook", "morning",
	"table", "snotty", "hesitant", "abusive", "short", "picture", "feeling", "lake", "digestion",
	"error", "bounce", "spark", "black", "friends", "cagey", "wide-eyed", "head", "teaching",
	"mess up", "parallel", "relieved", "remember", "grieving", "dirt", "inform", "vest", "clover",
	"marry", "hill", "blushing", "trousers", "vanish", "deer", "plain", "quarrelsome", "longing",
	"bouncy", "post", "wilderness", "gamy", "old", "question", "teeny-tiny", "offer", "untidy",
	"medical", "tightfisted",
}

func data(depth int) <-chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)
		if depth > 0 {
			for _, d := range d0 {
				for e := range data(depth - 1) {
					ch <- d + "-" + e
				}
			}
		} else {
			for _, d := range d0 {
				ch <- d
			}
		}
	}()
	return ch
}

// BucketPlace is the location of a bucket on the ring
type BucketPlace struct {
	place  uint64
	bucket int
}

// ConsistentHashRing is just a collection of BucketPlace(s), sorted by place
type ConsistentHashRing struct {
	buckets  []BucketPlace
	replicas int
}

// NewConsistentHashRing makes a new ring given a set of buckets and replicas.
// buckets should be a positive, non-zero number
// replicas will default to 200 if less than or equal to zero
func NewConsistentHashRing(buckets int, replicas int) *ConsistentHashRing {
	if replicas <= 0 {
		replicas = buckets * buckets
	}
	ring := make([]BucketPlace, buckets*replicas, buckets*replicas)
	for b := 0; b < buckets; b++ {
		for r := 0; r < replicas; r++ {
			place := placeUInt64N(uint64(b), r+1)
			ring[(b*replicas)+r] = BucketPlace{place, b}
		}
	}
	sort.Slice(ring, func(i, j int) bool {
		return ring[i].place < ring[j].place
	})
	return &ConsistentHashRing{ring, replicas}
}

// NextObject will return the next bucket number on the ring when given a location
func (ring *ConsistentHashRing) NextObject(location uint64) int {
	rr := ring.buckets
	i := sort.Search(
		len(rr),
		func(i int) bool { return rr[i].place >= location },
	)
	if i >= len(rr) {
		return rr[0].bucket
	}
	return rr[i].bucket
}

func placeUInt64N(o uint64, ix int) uint64 {
	if ix > 0 {
		b := make([]byte, 8)
		for ; ix >= 1; ix-- {
			binary.LittleEndian.PutUint64(b, o)
			o = xxhash.Checksum64(b)
		}
	}
	return o
}

func placeStringN(s string, ix int) uint64 {
	return placeUInt64N(xxhash.ChecksumString64(s), ix)
}

func placeString(s string) uint64 {
	return xxhash.ChecksumString64(s)
}

func uniformity(v []int) float64 {
	// https://stats.stackexchange.com/a/92056
	total := float64(0)
	for i := 0; i < len(v); i++ {
		total += float64(v[i])
	}
	sqrtD := math.Sqrt(float64(len(v)))
	l2nSq := 0.0
	for i := 0; i < len(v); i++ {
		l2nSq += math.Pow(float64(v[i])/total, 2)
	}
	l2n := math.Sqrt(l2nSq)
	return ((l2n * sqrtD) - 1.0) / (sqrtD - 1.0)
}

func sexyTime(t time.Duration) string {
	secs := t.Seconds()
	if secs > 1 {
		return fmt.Sprintf("%0.2fs", secs)
	}
	if secs > 0.001 {
		return fmt.Sprintf("%0.2fms", secs*1000)
	}
	if secs > 0.000001 {
		return fmt.Sprintf("%0.2fµs", secs*1000000)
	}
	return fmt.Sprintf("%dns", t.Nanoseconds())
}

func sexyHertz(hz float64) string {
	symbols := []string{"Hz", "KHz", "MHz", "GHz", "THz"}
	for i := 0; i < len(symbols); i++ {
		if hz < 1000 {
			return fmt.Sprintf("%0.2f%s", hz, symbols[i])
		}
		hz /= 1000
	}
	return fmt.Sprintf("%0.2fHz", hz)
}

func main() {
	maxLen := 20
	minLen := 1
	replicas := -1

	for i := maxLen; i >= minLen; i-- {
		buckets := make([]int, i, i)
		ring := NewConsistentHashRing(i, replicas)
		ring2 := NewConsistentHashRing(i+1, replicas)
		cnt := 0
		moved := int64(0)
		duration := time.Duration(0)

		for d := range data(2) {
			start := time.Now()
			location := placeString(d)
			duration += time.Now().Sub(start)
			bucket := ring.NextObject(location)
			bucket2 := ring2.NextObject(location)
			buckets[bucket]++
			cnt++
			if bucket != bucket2 {
				moved++
			}
		}

		diffbuckets := make([]int, i, i)
		for s := 0; s < len(ring2.buckets); s++ {
			if ring2.buckets[s].bucket == i {
				nextSlot := s
				// keep looking until we find a slot that won't be destroyed
				for ; ring2.buckets[nextSlot].bucket == i; nextSlot = (nextSlot + 1) % len(ring2.buckets) {
				}

				diffbuckets[ring2.buckets[nextSlot].bucket]++
			}
		}

		fmt.Printf(
			"%d total in %s (%s); %d (%0.2f%%, %0.2f%% theoretical) moved; %0.3f uniformity\n",
			cnt,
			sexyTime(duration),
			sexyHertz(float64(cnt)/(duration.Seconds())),
			moved,
			float64(moved)*100.0/float64(cnt),
			100.0/float64(i+1),
			(1.0-uniformity(buckets))*100.0,
		)
		for i := 0; i < len(buckets); i++ {
			if i > 0 {
				fmt.Print(", ")
			}
			fmt.Printf(strconv.Itoa(buckets[i]))
		}
		fmt.Println()
		for i := 0; i < len(diffbuckets); i++ {
			if i > 0 {
				fmt.Print(", ")
			}
			fmt.Printf("%0.2f%%", float64(diffbuckets[i])*100.0/float64(ring2.replicas))
		}
		fmt.Println()
		fmt.Println("-----")
	}
}
