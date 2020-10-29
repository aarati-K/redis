import numpy as np
import sys
import time
import struct

num_kv_to_generate = 3000000000 # 3 billion
batch_size = 100
num_batches = num_kv_to_generate/batch_size
dump_file = "/users/aarati_K/realhdd/kv_insert"
f = open(dump_file, 'wb')

start = time.time()
for i in range(num_batches):
    rns = np.random.randint(0, sys.maxint, batch_size, dtype=np.int64)
    for r in rns:
        # Convert to big endian format
        f.write(struct.pack('>q', r))
end = time.time()
print "Time taken:", end-start, "seconds"

f.close()
