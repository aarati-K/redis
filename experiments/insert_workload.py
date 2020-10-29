import numpy as np
import sys
import time
import redis

num_kv_to_generate = 3000000000 # 3 billion

# Number of random integers obtained in one call of np.random.randint
# For now, we use the same batch size to insert kv pairs using mset 
batch_size = 100
num_batches = num_kv_to_generate/batch_size

r = redis.Redis()

start = time.time()
try:
    for i in range(num_batches):
        rns = np.random.randint(0, sys.maxint, batch_size, dtype=np.int64)
        hm = {}
        for rn in rns:
            hm[rn] = rn
        r.mset(hm)
        if i % 10000 == 0:
            print "Batch", i, "done"
except:
    pass

end = time.time()
print "Last batch number:", i
print "Time taken:", end-start, "seconds"
