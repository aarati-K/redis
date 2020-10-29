import redis
import time
import sys
import random

KB = 1024
MB = 1024*KB

# 1 for bulk requests
# 2 for batched requests
# 3 for individual requests
insert_mode = 2
batch_size = 5

# Redis running on localhost, no args needed
r = redis.Redis()

num_keys_to_insert = 32260
hm = {}
batches = []
keys_to_insert = []

for i in range(num_keys_to_insert):
    x = random.randint(0, sys.maxint)
    keys_to_insert.append(x)
    hm[x] = x
    if i>0 and i%batch_size == batch_size-1:
        temp = {}
        for j in range(i-4, i+1):
            temp[keys_to_insert[j]] = keys_to_insert[j]
        batches.append(temp)

if len(batches) != num_keys_to_insert/batch_size:
    print "Something wrong with creating batches"

# Benchmarking INSERTs
if insert_mode == 1:
    # Measure the duration of bulk insert operation
    start = time.time()
    r.mset(hm)
    end = time.time()
    # Took 1.76 seconds on the cloudlab machine
    # Will this be enough to keep up with 1 million requests per minute?
    # Will the RTT overhead mask the performance gains?
    print "Time taken to insert in bulk:", end-start, "seconds"
elif insert_mode == 2:
    # Perform insert in batches of batch_size
    start = time.time()
    for b in batches:
        r.mset(b)
    end = time.time()
    # Took 0.5 seconds to run. Seems like a reasonable compromise.
    print "Time taken to insert in batches of 5:", end-start, "seconds"
elif insert_mode == 3:
    # Measure the duration of individual set requests
    start = time.time()
    for i in keys_to_insert:
        r.set(i, i)
    end = time.time()
    # Took 0.19 seconds on the cloudlab machine
    print "Time taken to insert one at a time:", end-start, "seconds"
