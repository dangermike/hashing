#!/usr/bin/env python3
from __future__ import annotations
from xxhash import xxh64
from typing import Collection, Iterable, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer

d0 = [
    'spiffy', 'amusing', 'weigh', 'milk', 'groan', 'utter', 'low', 'abusive',
    'fill', 'spark', 'important', 'joke', 'snail', 'crib', 'chalk', 'group',
    'pull', 'impress', 'capable', 'design', 'fry', 'authority', 'exclusive',
    'nutritious', 'robin', 'book', 'upbeat', 'smoke', 'oval', 'sparkling',
    'available', 'domineering', 'treatment', 'friends', 'alert', 'occur',
    'level', 'old-fashioned', 'unadvised', 'crabby', 'languid', 'radiate',
    'wine', 'pest', 'behavior', 'drown', 'eggs', 'tasteless', 'check', 'peace'
]


def data(depth: int = 2) -> Iterable[str]:
    if depth > 0:
        for x in data(depth=depth - 1):
            for y in d0:
                yield x + '-' + y
    else:
        for x in d0:
            yield x


def place_object(o: Union[ReadableBuffer, str, int], ix=0) -> int:
    if isinstance(o, int):
        o = o.to_bytes(8, byteorder='big')
    d = xxh64(o).intdigest()
    if ix > 0:
        d = place_object(d + ix, ix=ix - 1)
    return d


class ConsistentHashRing(object):

    def __init__(self,
                 buckets: int,
                 replicas: int = 200,
                 place_func=place_object):
        ring = []
        self.place_func = place_func
        for b in range(buckets):
            for x in range(replicas):
                ring.append((self.place_func(b, x), b))
        self._ring = sorted(ring)

    def next_object(self, loc: int) -> int:
        n = 0
        m = len(self._ring) - 1
        if loc > self._ring[m][0] or loc < self._ring[n][0]:
            return self._ring[0][1]
        # estimate original position -- we know the keyspace is 0 -> 1<<64
        x = loc * len(self._ring) // (1 << 64)
        while m - n > 1:
            if self._ring[x][0] == loc:
                return self._ring[x][1]
            elif self._ring[x][0] > loc:
                m = x
            else:  # ring[x][0] < loc
                n = x
            x = (m + n) // 2
        return self._ring[m][1]

    def target_bucket(self,
                      o: Union[ReadableBuffer, str, int],
                      ix: int = 0) -> int:
        loc = self.place_func(o, ix=ix)
        return self.next_object(loc)


def uniformity(v: Collection[int]):
    # https://stats.stackexchange.com/a/92056
    total = sum(v)
    sqrt_d = len(v)**0.5
    normalized_v = [i / total for i in v]
    l2n = sum([(i)**2 for i in normalized_v])**0.5
    return ((l2n * sqrt_d) - 1) / (sqrt_d - 1)


rings = []
placements = []
for ring_cnt in range(10, 5, -1):
    rings.append(ConsistentHashRing(ring_cnt))
    placements.append([0] * ring_cnt)
# for r in rings:
#     print("".join([str(x[1]) for x in r._ring]))

moved = [0] * len(rings)
total = 0

for s in data(1):
    total += 1
    o = [ring.target_bucket(s) for ring in rings]
    for i in range(len(o)):
        placements[i][o[i]] += 1
        if i > 0 and o[i] != o[i - 1]:
            moved[i] += 1
    # print("{0:20s} ({1:10d}): {2}".format(s, p, o))

for i in range(len(placements)):
    print(placements[i])
    print("{0:.2f}% moved ({1} of {2}, {3:.2f}% theoretical)".format(
        moved[i] * 100 / total, moved[i], total,
        100 / (len(placements[i]) + 1)))
    print("{0:.3f}% uniform".format((1 - uniformity(placements[i])) * 100))
