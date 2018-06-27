#!/usr/bin/env python3
import binascii


class AwfulHash64(object):
    def __init__(self, data=None):
        self._hashval = 0b1111111111111101100110011010000110010111100000101100100011001001
        self._remnant = bytes(0)
        if data:
            self.update(data)

    @property
    def name():
        return 'AwfulHash64'

    def update(self, data):
        while len(data) >= 8:
            new_bytes = self._remnant + data[0:8 - len(self._remnant)]
            self._hashval ^= int.from_bytes(new_bytes, byteorder='big')
            data = data[8:]

        self._remnant = data
        return self

    def digest(self):
        final_val = \
            self._hashval ^ \
            int.from_bytes(self._remnant + bytes(8-len(self._remnant)), byteorder='big')
        return final_val.to_bytes(8, byteorder='big')

    def hexdigest(self):
        return binascii.hexlify(self.digest()).decode('utf-8')


class BarfHash64(object):
    def __init__(self, data=None):
        self._hashval = 0b1111111111111101100110011010000110010111100000101100100011001001
        self._remnant = bytes(0)
        self._blocks = 0
        if data:
            self.update(data)

    @property
    def name():
        return 'BarfHash64'

    @staticmethod
    def _shift(base, n):
        shift = (11 * n) % 64
        return ((base << shift) & 0xFFFFFFFFFFFFFFFF) | (base >> (64 - shift))

    def update(self, data):
        while len(data) >= 8:
            self._blocks += 1
            new_bytes = self._remnant + data[0:8 - len(self._remnant)]
            self._hashval ^= int.from_bytes(new_bytes, byteorder='big')
            self._hashval = BarfHash64._shift(self._hashval, self._blocks)
            data = data[8:]

        self._remnant = data
        return self

    def digest(self):
        final_val = BarfHash64._shift(
            self._hashval ^ int.from_bytes(self._remnant + bytes(8-len(self._remnant)), byteorder='big'),
            self._blocks + 1
        )
        return final_val.to_bytes(8, byteorder='big')

    def hexdigest(self):
        return binascii.hexlify(self.digest()).decode('utf-8')


class AwfulHashLib(object):
    def __init__():
        pass

    @staticmethod
    def new(name, data=None):
        name = name.lower()
        if name == 'awfulhash64':
            h = AwfulHash64()
            if data:
                h.update(data)
            return h
