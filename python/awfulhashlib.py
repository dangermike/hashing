#!/usr/bin/env python3
from __future__ import annotations
import binascii
import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from _typeshed import ReadableBuffer


class AwfulHash64(object):

    def __init__(self, data: ReadableBuffer = None):
        self._hashval = 0b1111111111111101100110011010000110010111100000101100100011001001
        self._remnant = bytearray(0)
        if data:
            self.update(data)

    @property
    def digest_size(self) -> int:
        return 8

    @property
    def block_size(self) -> int:
        return 8

    @property
    def name(self) -> str:
        return 'AwfulHash64'

    def _remnant_val(self) -> int:
        return int.from_bytes(
            self._remnant, byteorder='big') << (64 - (len(self._remnant) * 8))

    def copy(self) -> AwfulHash64:
        h = AwfulHash64()
        h._hashval = self._hashval
        h._remnant = self._remnant.copy()

        return h

    def digest(self) -> bytes:
        final_val = \
            self._hashval ^ self._remnant_val()
        return final_val.to_bytes(8, byteorder='big')

    def hexdigest(self) -> str:
        return binascii.hexlify(self.digest()).decode('utf-8')

    def update(self, data: ReadableBuffer):
        mv = memoryview(data)
        nb = mv.nbytes
        start = 0
        while start + 8 < nb:
            if len(self._remnant) > 0:
                # read the remnant
                val = self._remnant_val()
                to_read = 8 - len(self._remnant)
                val += int.from_bytes(mv[start:start + to_read],
                                      byteorder='big')
                start += to_read
                self._remnant.clear()
            else:
                val = int.from_bytes(mv[start:start + 8], byteorder='big')
                start += 8

            self._hashval ^= val

        if start < nb:
            self._remnant[0:nb - start] = mv[start:]


class BarfHash64(object):

    def __init__(self, data: ReadableBuffer = None):
        self._hashval = 0b1111111111111101100110011010000110010111100000101100100011001001
        self._remnant = bytearray(0)
        self._blocks = 0
        if data:
            self.update(data)

    @property
    def digest_size(self) -> int:
        return 8

    @property
    def block_size(self) -> int:
        return 8

    @property
    def name(self) -> str:
        return 'BarfHash64'

    def _remnant_val(self) -> int:
        return int.from_bytes(
            self._remnant, byteorder='big') << (64 - (len(self._remnant) * 8))

    def copy(self) -> BarfHash64:
        h = BarfHash64()
        h._hashval = self._hashval
        h._remnant = self._remnant.copy()
        h._blocks = self._blocks

        return h

    def digest(self) -> bytes:
        final_val = \
            self._hashval ^ self._remnant_val()
        return final_val.to_bytes(8, byteorder='big')

    def hexdigest(self) -> str:
        return binascii.hexlify(self.digest()).decode('utf-8')

    def update(self, data: ReadableBuffer):
        mv = memoryview(data)
        nb = mv.nbytes
        start = 0
        while start + 8 < nb:
            if len(self._remnant) > 0:
                # read the remnant
                val = self._remnant_val()
                to_read = 8 - len(self._remnant)
                val += int.from_bytes(mv[start:start + to_read],
                                      byteorder='big')
                start += to_read
                self._remnant.clear()
            else:
                val = int.from_bytes(mv[start:start + 8], byteorder='big')
                start += 8

            self._blocks += 1
            self._hashval ^= val
            self._hashval = BarfHash64._shift(self._hashval, self._blocks)

        if start < nb:
            self._remnant[0:nb - start] = mv[start:]

    @staticmethod
    def _shift(base: int, n: int) -> int:
        shift = (11 * n) % 64
        return ((base << shift) & 0xFFFFFFFFFFFFFFFF) | (base >> (64 - shift))


class AwfulHashLib(object):

    def __init__(self):
        pass

    @staticmethod
    def new(name: str, data: ReadableBuffer = b'', **kwargs):
        name = name.lower()
        if name == 'awfulhash64':
            return AwfulHash64(data)
        if name == 'barfhash64':
            return BarfHash64(data)
        return hashlib.new(name, data, **kwargs)
