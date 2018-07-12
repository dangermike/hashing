#!/usr/bin/env python3
from xxhash import xxh64

d0 = [
    'spiffy', 'amusing', 'weigh', 'milk', 'groan', 'utter', 'low', 'abusive',
    'fill', 'spark', 'important', 'joke', 'snail', 'crib', 'chalk', 'group',
    'pull', 'impress', 'capable', 'design', 'fry', 'authority', 'exclusive',
    'nutritious', 'robin', 'book', 'upbeat', 'smoke', 'oval', 'sparkling',
    'available', 'domineering', 'treatment', 'friends', 'alert', 'occur',
    'level', 'old-fashioned', 'unadvised', 'crabby', 'languid', 'radiate',
    'wine', 'pest', 'behavior', 'drown', 'eggs', 'tasteless', 'check', 'peace'
    ]


def data(depth=2):
    if depth > 0:
        for x in data(depth=depth-1):
            for y in d0:
                yield x + '-' + y
    else:
        for x in d0:
            yield x


class HyperLogLog(object):
    def __init__(self, bucket_bits=10):
        self.table = [0] * (1 << bucket_bits)
        self.bucket_bits = bucket_bits
        self.m = 1 << self.bucket_bits
        self.key_filter = (1 << (64 - bucket_bits)) - 1

    def hash_val(self, o):
        if isinstance(o, int):
            o = o.to_bytes(8, byteorder='big')
        d = xxh64(o).intdigest()
        return d

    def add(self, value):
        if value is None:
            return
        key = self.hash_val(value)
        bucket_ix = key >> (64-self.bucket_bits)
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
    def count(self):
        z = 1.0 / sum([1.0/(1 << x) for x in self.table])

        # shameless thievery. ok, maybe a little shame
        if self.m == 16:
            alpha = 0.673
        elif self.m == 32:
            alpha = 0.697
        elif self.m == 64:
            alpha = 0.709
        else:
            alpha = 0.7213/(1 + 1.079/self.m)

        print(z)
        return alpha * (self.m ** 2) * z

    @property
    def error(self):
        return 1.3 / (self.bucket_bits ** 0.5)


actual = 0
hll = HyperLogLog(3)
for x in data(2):
    actual += 1
    hll.add(x)

print(hll.table)

print(
    "actual: %d, hll est: %0.2f, hll err: %0.2f" %
    (actual, hll.count, hll.error)
)
