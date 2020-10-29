import redis
import time
import sys
import random

KB = 1024
MB = 1024*KB

bulk_insert = True

# Redis running on localhost, no args needed
r = redis.Redis()

num_keys_to_insert = 32260
hm = {}
keys_to_insert = []

for i in range(num_keys_to_insert):
    x = random.randint(0, sys.maxint)
    keys_to_insert.append(x)
    hm[x] = x

if bulk_insert:
    # Measure the duration of bulk insert operation
    start = time.time()
    r.mset(hm)
    end = time.time()
    print "Time taken to insert in bulk:", end-start, "seconds"
else:
    # Measure the duration of individual set requests
    start = time.time()
    for i in keys_to_insert:
        r.set(i, i)
    end = time.time()
    print "Time taken to insert one at a time:", end-start, "seconds"

