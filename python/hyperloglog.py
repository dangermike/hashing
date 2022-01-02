#!/usr/bin/env python3
from __future__ import annotations
from xxhash import xxh64
from typing import Iterator, List, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer

d0: List[str] = [
    'spiffy', 'amusing', 'weigh', 'milk', 'groan', 'utter', 'low', 'abusive',
    'fill', 'spark', 'important', 'joke', 'snail', 'crib', 'chalk', 'group',
    'pull', 'impress', 'capable', 'design', 'fry', 'authority', 'exclusive',
    'nutritious', 'robin', 'book', 'upbeat', 'smoke', 'oval', 'sparkling',
    'available', 'domineering', 'treatment', 'friends', 'alert', 'occur',
    'level', 'old-fashioned', 'unadvised', 'crabby', 'languid', 'radiate',
    'wine', 'pest', 'behavior', 'drown', 'eggs', 'tasteless', 'check', 'peace'
]


def data(depth: int = 2) -> Iterator[str]:
    if depth > 0:
        for x in data(depth=depth - 1):
            for y in d0:
                yield x + '-' + y
    else:
        for x in d0:
            yield x


class HyperLogLog(object):

    def __init__(self, bucket_bits: int = 10):
        self.table = [0] * (1 << bucket_bits)
        self.bucket_bits = bucket_bits
        self.m = 1 << self.bucket_bits
        self.key_filter = (1 << (64 - bucket_bits)) - 1

    @staticmethod
    def hash_val(o: Union[ReadableBuffer, str, int]) -> int:
        if isinstance(o, int):
            o = o.to_bytes(8, byteorder='big')
        d = xxh64(o).intdigest()
        return d

    def add(self, value: Union[ReadableBuffer, str, int]):
        if value is None:
            return
        key = HyperLogLog.hash_val(value)
        bucket_ix = key >> (64 - self.bucket_bits)
        value = key & self.key_filter
        zeros = 0
        data_bits = 64 - self.bucket_bits
        while 0 == value >> (data_bits - (zeros + 1)):
            zeros += 1
        zeros += 1
        # print(zeros)
        if zeros > self.table[bucket_ix]:
            self.table[bucket_ix] = zeros

    @property
    def alpha(self) -> float:
        # shameless thievery. ok, maybe a little shame
        if self.m == 16:
            return 0.673
        elif self.m == 32:
            return 0.697
        elif self.m == 64:
            return 0.709
        return 0.7213 / (1 + (1.079 / self.m))

    @property
    def count(self) -> float:
        z = 1.0 / sum([1.0 / (1 << x) for x in self.table])
        return self.alpha * (self.m**2) * z

    @property
    def error(self) -> float:
        return 1.04 / (self.bucket_bits**0.5)


actual = 0
hll = HyperLogLog(16)
for x in data(2):
    actual += 1
    hll.add(x)

# print(hll.table)

print("actual: %d, hll est: %0.2f, est err: %0.2f, actual err: %0.2f" %
      (actual, hll.count, hll.error, (hll.count / actual) - 1.0))
