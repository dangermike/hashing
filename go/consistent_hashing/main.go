package main

import (
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/dangermike/hashing/go/consistent_hashing/ConsistentHashing"
	"github.com/dangermike/hashing/go/consistent_hashing/JumpHash"
	"github.com/dangermike/hashing/go/consistent_hashing/MultiPointHashing"
	"github.com/dangermike/hashing/go/consistent_hashing/ObjectHasher"
	"github.com/dangermike/hashing/go/consistent_hashing/RendezvousHashing"
	"github.com/dangermike/hashing/go/consistent_hashing/RendezvousHashingWithSkeleton"
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

type mapper interface {
	MapBucket(location uint64) int
	ExpectedMoveRate(otherSize int) float64
	Name() string
}

func main() {
	maxLen := 20
	minLen := 10
	replicas := 200

	for i := maxLen; i >= minLen; i-- {
		targets := [][2]mapper{
			[2]mapper{
				JumpHash.New(i),
				JumpHash.New(i + 1),
			},
			[2]mapper{
				MultiPointHashing.New(i, 10),
				MultiPointHashing.New(i+1, 10),
			},
			[2]mapper{
				JumpHash.New(i),
				JumpHash.New(i + 1),
			},
			[2]mapper{
				ConsistentHashing.New(i, replicas),
				ConsistentHashing.New(i+1, replicas),
			},
			[2]mapper{
				RendezvousHashing.New(i),
				RendezvousHashing.New(i + 1),
			},
			[2]mapper{
				RendezvousHashingWithSkeleton.New(i, 4, 3),
				RendezvousHashingWithSkeleton.New(i+1, 4, 3),
			},
			[2]mapper{
				RendezvousHashingWithSkeleton.New(i, i, i),
				RendezvousHashingWithSkeleton.New(i+1, i+1, i+1),
			},
		}

		for _, mappers := range targets {
			fmt.Println(mappers[0].Name())
			buckets := make([]int, i, i)
			cnt := 0
			moved := int64(0)
			duration := time.Duration(0)

			for d := range data(2) {
				start := time.Now()
				location := ObjectHasher.PlaceString(d)
				bucket := mappers[0].MapBucket(location)
				duration += time.Now().Sub(start)
				bucket2 := mappers[1].MapBucket(location)
				buckets[bucket]++
				cnt++
				if bucket != bucket2 {
					moved++
				}
			}

			fmt.Println(Sizeof(mappers[0]))
			fmt.Printf(
				"%d total in %s (%s); %d (%0.2f%%, %0.2f%% theoretical) moved; %0.3f uniformity\n",
				cnt,
				sexyTime(duration),
				sexyHertz(float64(cnt)/(duration.Seconds())),
				moved,
				float64(moved)*100.0/float64(cnt),
				100.0*mappers[0].ExpectedMoveRate(i+1),
				(1.0-uniformity(buckets))*100.0,
			)
			// for i := 0; i < len(buckets); i++ {
			// 	if i > 0 {
			// 		fmt.Print(", ")
			// 	}
			// 	fmt.Printf(strconv.Itoa(buckets[i]))
			// }

			// fmt.Println()
			fmt.Println("-----")
		}
	}
}

var (
	sliceSize  = uint64(reflect.TypeOf(reflect.SliceHeader{}).Size())
	stringSize = uint64(reflect.TypeOf(reflect.StringHeader{}).Size())
)

func isNativeType(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return true
	}
	return false
}

func sizeofInternal(val reflect.Value, fromStruct bool, depth int) (sz uint64) {
	if depth++; depth > 1000 {
		panic("sizeOf recursed more than 1000 times.")
	}

	typ := val.Type()

	if !fromStruct {
		sz = uint64(typ.Size())
	}

	switch val.Kind() {
	case reflect.Ptr:
		if val.IsNil() {
			break
		}
		sz += sizeofInternal(val.Elem(), false, depth)

	case reflect.Struct:
		for i := 0; i < val.NumField(); i++ {
			sz += sizeofInternal(val.Field(i), true, depth)
		}

	case reflect.Array:
		if isNativeType(typ.Elem().Kind()) {
			break
		}
		sz = 0
		for i := 0; i < val.Len(); i++ {
			sz += sizeofInternal(val.Index(i), false, depth)
		}
	case reflect.Slice:
		if !fromStruct {
			sz = sliceSize
		}
		el := typ.Elem()
		if isNativeType(el.Kind()) {
			sz += uint64(val.Len()) * uint64(el.Size())
			break
		}
		for i := 0; i < val.Len(); i++ {
			sz += sizeofInternal(val.Index(i), false, depth)
		}
	case reflect.Map:
		if val.IsNil() {
			break
		}
		kel, vel := typ.Key(), typ.Elem()
		if isNativeType(kel.Kind()) && isNativeType(vel.Kind()) {
			sz += uint64(kel.Size()+vel.Size()) * uint64(val.Len())
			break
		}
		keys := val.MapKeys()
		for i := 0; i < len(keys); i++ {
			sz += sizeofInternal(keys[i], false, depth) + sizeofInternal(val.MapIndex(keys[i]), false, depth)
		}
	case reflect.String:
		if !fromStruct {
			sz = stringSize
		}
		sz += uint64(val.Len())
	}
	return
}

// Sizeof returns the estimated memory usage of object(s) not just the size of the type.
// On 64bit Sizeof("test") == 12 (8 = sizeof(StringHeader) + 4 bytes).
func Sizeof(objs ...interface{}) (sz uint64) {
	for i := range objs {
		sz += sizeofInternal(reflect.ValueOf(objs[i]), false, 0)
	}
	return
}
