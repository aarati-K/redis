import numpy as np
import sys
import time
import redis
import random

# Phase 1: warmup

# 250 million keys to insert during warmup
# They generate an rdb file of about 9GB
warmup_filename = "/users/aarati_K/hdd/ETC/warmup"
f = open(warmup_filename, "w")
num_kv_to_generate = 250000000
keys_inserted = []
batch_size = 100
num_batches = num_kv_to_generate/batch_size

start = time.time()
try:
    for i in range(num_batches):
        rns = np.random.randint(0, sys.maxint, batch_size, dtype=np.int64)
        keys_inserted.extend(rns)
        for rn in rns:
            f.write("{}\n".format(str(rn)))
except:
    pass
end = time.time()
print "Time taken to generate warmup keys:", end-start, "seconds"
print
f.close()

# Phase 2: ETC workload - Generate total 1B requests
# 30:1 ratio of GET:SET
# Batch size: 10? (review this choice later)
# Same batch size for both GET and SET
# SET requests require sampling of new keys
# GET requests need to follow a zipfian, right now I use a bucketed zipfian
num_requests_to_generate = 1000060000
num_set_requests = num_requests_to_generate/31 # 32.26 million
num_get_requests = num_requests_to_generate - num_set_requests # 967.8 million
batch_size = 10

# approximate zipfian
# There are 2.5 million buckets, each with a 100 keys initially (total 250M keys)
# Dynamically allocate newly inserted keys to these buckets
# Bucket size will become approx 250 by the end of the experiment
bucket_size = 100
num_buckets = len(keys_inserted)/bucket_size
random.shuffle(keys_inserted)
buckets = []
for i in range(num_buckets):
    bucket = keys_inserted[i*bucket_size:(i+1)*bucket_size]
    buckets.append(bucket)
# Sanity check
if num_buckets != len(buckets):
    print "Something wrong with bucket generation, needed:", \
        num_buckets, "generated:", len(buckets)

if len(buckets[0]) != bucket_size:
    print "Bucket 0 doesn't match desired size"

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

# We will have 2000 iterations of approx 0.5 million requests each
# Overall 1B requests
num_iterations = 200
num_requests_per_iteration = num_requests_to_generate/num_iterations # 5000300
num_get_requests_per_iteration = num_get_requests/num_iterations # 4839000

# For each get request in an iteration choose a bucket
# First calculate the frequency of occurence of each bucket 
bucket_freq = [0]*num_buckets
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
for i in range(10000000):
    random_choice_array.append(random.randint(0, 1073741824)) # 2**30 = 1073741824
rlen = len(random_choice_array)

insert_filename = "/users/aarati_K/hdd/ETC/insert"
fetch_filename = "/users/aarati_K/hdd/ETC/fetch"
f1 = open(insert_filename, "w")
f2 = open(fetch_filename, "w")

# 200 * 1613 * 3100 = 1000060000 (1000.006 million requests total)
for i in range(num_iterations):
    for j in range(1613):
        # Batch size = 10, 310 batches of requests (3100 requests total)
        # 10 batchs (= 100) of SET requests
        for k in range(10):
            rns = np.random.randint(0, sys.maxint, batch_size, dtype=np.int64)
            for rn in rns:
                f1.write("{}\n".format(str(rn)))
                rand_bucket = random_choice_array[rit] % num_buckets
                rit = (rit+1)%rlen
                buckets[rand_bucket].append(rn)

        # 300 batches (= 300) GET requests
        for k in range(3000):
            bid = bucket_choice[j*3000+k]
            bucket_len = len(buckets[bid])
            rand_bucket_pos = random_choice_array[rit]%bucket_len
            rand_key = buckets[bid][rand_bucket_pos]
            f2.write("{}\n".format(str(rand_key)))

f1.close()
f2.close()
