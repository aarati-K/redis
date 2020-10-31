import numpy as np
import sys
import time
import redis

############################################
########### DATA LOADING ###################
############################################

# 250 million keys to insert during warmup
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

num_kv_to_warmup = 250000000
if len(keys_inserted) != num_kv_to_warmup:
    print "Mismatched number of keys for warmup"

# Our goal is to generate 1 billion requests using a single client
# NOTE: we reduce the number of iterations from 50 to 200
# So actually only 1/4th of these arrays will be used
num_requests_to_execute = 1000060000
num_set_requests = int(num_requests_to_execute/31.0) # 3.226 million
num_get_requests = num_requests_to_execute - num_set_requests # 96.78 million

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

############################################
########### PHASE 1: WARMUP   ##############
############################################
batch_size = 100
num_batches = num_kv_to_warmup/batch_size

r = redis.Redis()

start = time.time()
try:
    for i in range(num_batches):
        rns = keys_inserted[i*batch_size:(i+1)*batch_size]
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

############################################
######## PHASE 2: ETC Workload #############
############################################

# Phase 2: ETC workload
# 30:1 ratio of GET:SET
# Batch size: 10? (review this choice later)
# Same batch size for both GET and SET
# SET requests require sampling of new keys
# GET requests need to follow a zipfian
# We model an approximate zipfian

# Issue approx 50 million requests in each loop
# We run the loop a 50 times, so 250M requests overall
num_iterations = 50
num_get_batches = 300
num_set_batches = 10
batch_size = 10
set_latencies = []
get_latencies = []
set_batch_offset = 0
get_batch_offset = 0
# 50 * 1613 * 3100 = 250015000 (250.015 million requests total)
for i in range(num_iterations):
    if i%10 == 0:
        print "Iteration", i
    for j in range(1613):
        # Batch size is 10 for both set and get requests
        # Issue 300 get batches, and 10 set batch

        # SET requests
        for k in range(10):
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
        for k in range(300):
            rns = get_order[get_batch_offset:get_batch_offset+batch_size]
            get_batch_offset += batch_size
            hm = {}
            for rn in rns:
                hm[rn] = rn
            start = time.time()
            r.mget(hm)
            end = time.time()
            get_latencies.append(end-start)

time.sleep(120)

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
