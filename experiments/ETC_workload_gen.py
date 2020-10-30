import numpy as np
import sys
import time
import redis
import struct

# Phase 1: warmup

# 2.5 million keys to insert during warmup
# They generate an rdb file of about 9GB
warmup_filename = "/users/aarati_K/hdd/ETC/warmup"
f = open(warmup_filename, "wb")
num_kv_to_generate = 2500000
keys_inserted = []
batch_size = 100
num_batches = num_kv_to_generate/batch_size

start = time.time()
try:
    for i in range(num_batches):
        rns = np.random.randint(0, sys.maxint, batch_size, dtype=np.int64)
        keys_inserted.extend(rns)
        for rn in rns:
            f.write(struct.pack('>q', rn))
except:
    pass
end = time.time()
print "Time taken to generate warmup keys:", end-start, "seconds"
print
f.close()

# Phase 2: ETC workload - Generate total 50M requests
# 30:1 ratio of GET:SET
# Batch size: 10? (review this choice later)
# Same batch size for both GET and SET
# SET requests require sampling of new keys
# GET requests need to follow a zipfian, right now I use a bucketed zipfian
num_requests_to_generate = 50003000
num_set_requests = num_requests_to_generate/31 # 1.613 million
num_get_requests = num_requests_to_generate - num_set_requests # 48.39 million
batch_size = 10

# approximate zipfian
# There are 25000 buckets, each with a 100 keys initially (total 2.5M keys)
# Dynamically allocate newly inserted keys to these buckets
# Bucket size will become approx 150 by the end of the experiment
bucket_size = 100
num_buckets = len(keys_inserted)/bucket_size
random.shuffle(keys_inserted)
buckets = []
for i in range(num_buckets):
    bucket = keys_inserted[i*bucket_size:((i+1)*bucket_size-1)]
    buckets.append(bucket)
# Sanity check
if num_buckets != len(buckets):
    print "Something wrong with bucket generation, needed:", 
        num_buckets, "generated:", len(buckets)

# Calculate probability of each bucket
probabilities = [0]*num_buckets
zipf = 1
for i in range(num_buckets):
    probabilities[i] = 1/float(pow(i+1, zipf))
total = sum(probabilities)
for i in range(num_buckets):
    probabilities[i] = probabilities[i]/total
total = sum(probabilities)
probabilities[0] += 1-total

# We will have 100 iterations of approx 0.5 million requests each
num_iterations = 100
num_requests_per_iteration = num_requests_to_generate/num_iterations # 500030
num_get_requests_per_iteration = num_get_requests/num_iterations # 483900

# For each get request in an iteration choose a bucket
# First calculate the frequency of occurence of each bucket 
bucket_freq = []
for i in range(num_buckets):
    bucket_freq[i] = int(num_get_requests_per_iteration*probabilities[i])
total = sum(bucket_freq)
bucket_freq[0] += num_get_requests_per_iteration-total

# Generate bucket assignment for each get request
bucket_choice = []
for i in range(num_buckets):
    bucket_choice.extend([i]*bucket_freq[i])

if len(bucket_choice) != num_get_requests_per_iteration:
    print "Something wrong with bucket choice generation code"
    print "Generated:", len(bucket_choice), "Required:", num_get_requests_per_iteration
random.shuffle(bucket_choice)

# pseudo randomness, for choosing an entry in the bucket later on
# Each bucket is of size 100-200 during the experiment
random_choice_array = []
rit = 0 # iterator for this array
for i in range(100000):
    random_choice_array.append(random.randint(0, 1048576)) # 2**20 = 1048576
rlen = len(random_choice_array)

insert_filename = "/users/aarati_K/hdd/ETC/insert"
fetch_filename = "/users/aarati_K/hdd/ETC/insert"
f1 = open(insert_filename, "wb")
f2 = open(fetch_filename, "wb")

# 100 * 1613 * 310 = 50003000 (50.003 million requests total)
for i in range(100):
    for j in range(1613):
        # Batch size = 10, 31 batches of requests (310 requests total)
        # 1 batch (= 10) of SET requests
        rns = np.randint(0, sys.maxint, batch_size, dtype=np.int64)
        for rn in rns:
            f1.write(struct.pack('>q', rn))
            rand_bucket = random_choice_array[rit] % num_buckets
            rit = (rit+1)%rlen
            buckets[rand_bucket] = buckets[rand_bucket].append(rn)

        # 30 batches (= 300) GET requests
        for k in range(300):
            bid = bucket_choice[j*300+k]
            bucket_len = len(buckets[bid])
            rand_bucket_pos = random_choice_array[rit]%bucket_len
            rand_key = buckets[bid][rand_bucket_pos]
            f2.write(struct.pack('>q', rand_key))

f1.close()
f2.close()
