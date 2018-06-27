# Hashing

This repo is not meant to stand on its own, but rather is a collection of random
bits of code related to a presentation on hashing.

# Components
## Consistent hashing
A simple but effective implementation of a consistent hash ring used to place
strings into buckets. Measures uniformity as well as tracking the number of
elements that move as buckets are taken off the ring.

The measure of uniformity is a known area for improvement. A better measure
might be a chi-squared test against the known uniform distribution with a
p-value that has meaning, rather than the normalized L2 norm thing we are
doing now.

## AwfulHash64
A really bad hashing function. Meant to illustrate the various measures of hash
quality (avalanche, positional, distribution, stability). A catalog of what not
to do and why one should cavalierly attempt to implement their own hash
functions.

## BarfHash64
An improvement on AwfulHash64, adds a rotation on each 8-byte cycle. Not quite
as bad for large inputs as AwfulHash64, but still pretty bad.

## TODO:
* **Vomitorium** a key derivation function based on BarfHash64
* **HMAC-XXX** A generic implementation of HMAC that will take in whatever hash
  function you like. Should be compatible with real-world HMAC implementations.
