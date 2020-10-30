import numpy as np
import sys
import time
import redis

# Phase 1: warmup

# 2.5 million keys to insert during warmup
# They generate an rdb file of about 9GB
warmup_filename = "/users/aarati_K/hdd/ETC/warmup"

f = open(warmup_filename, "r")
# We are yet to insert them, but calling them so
keys_inserted = []
for line in f:
    rn = int(line)
    keys_inserted.append(rn)

print len(keys_inserted)
f.close()

num_kv_to_warmup = 2500000
if len(keys_inserted) != num_kv_to_warmup:
    print "Mismatched number of keys for warmup"

batch_size = 100
num_batches = num_kv_to_warmup/batch_size

r = redis.Redis()

start = time.time()
try:
    for i in range(num_batches):
        rns = num_kv_to_warmup[i*batch_size:(i+1)*batch_size]
        hm = {}
        for rn in rns:
            hm[rn] = rn
        r.mset(hm)
        if i % 100000 == 0:
            print "Batch", i, "done"
except:
    pass
end = time.time()
print "Num keys inserted during warmup:", num_kv_to_warmup
print "Time taken for warmup:", end-start, "seconds"
print

# Phase 2: ETC workload
# 30:1 ratio of GET:SET
# Batch size: 10? (review this choice later)
# Same batch size for both GET and SET
# SET requests require sampling of new keys
# GET requests need to follow a zipfian
# We model an approximate zipfian

# Our goal is to generate 50 million requests using a single client
num_requests_to_generate = 100006000
num_set_requests = int(num_requests_to_generate/31.0) # 3.226 million
num_get_requests = num_requests_to_generate - num_set_requests # 96.78 million

insert_filename = "/users/aarati_K/hdd/ETC/insert"
fetch_filename = "/users/aarati_K/hdd/ETC/fetch"

f = open(insert_filename, 'r')
set_order = []
for line in f:
    set_order.append(int(line))
if len(set_order) != num_set_requests:
    print "Mismatched set random numbers"
f.close()

f = open(fetch_filename, 'r')
get_order = []
for line in f:
    get_order.append(int(line))
if len(get_order) != num_get_requests:
    print "Mismatched get random requests"
f.close()

# Issue approx 0.5 million requests in each loop
# We run the loop a 100 times, so 50M requests overall
num_requests_per_iter = 500030 # Made this a multiple of 310 for convenience
num_iterations = 200
num_get_batches = 30
num_set_batches = 1
batch_size = 10
set_latencies = []
get_latencies = []
set_batch_offset = 0
get_batch_offset = 0
# 200 * 1613 * 310 = 100006000 (100.006 million requests total)
for i in range(num_iterations):
    for j in range(1613):
        # Batch size is 10 for both set and get requests
        # Issue 30 get batches, and 1 set batch

        # SET request
        rns = set_order[set_batch_offset:set_batch_offset+batch_size]
        set_batch_offset += batch_size
        hm = {}
        for rn in rns:
            hm[rn] = rn
        start = time.time()
        r.mset(hm)
        end = time.time()
        set_latencies.append(end-start)

        # GET requests
        for k in range(30):
            rns = get_order[get_batch_offset:get_batch_offset+batch_size]
            get_batch_offset += batch_size
            hm = {}
            for rn in rns:
                hm[rn] = rn
            start = time.time()
            r.mget(hm)
            end = time.time()
            get_latencies.append(end-start)

sleep(120)

# Write measured latencies to file
set_results_file = "/users/aarati_K/hdd/ETC/latency_insert"
f = open(set_results_file, 'w')
for l in set_latencies:
    f.write("{}\n".format(l))
f.close()

get_results_file = "/users/aarati_K/hdd/ETC/latency_fetch"
f = open(get_results_file, 'w')
for l in get_latencies:
    f.write("{}\n".format(l))
f.close()
