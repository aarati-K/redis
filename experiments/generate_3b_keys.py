import random
import sys
import time
import math
from Crypto.Random.random import randint

def onthefly(n):
    # Array of 1...n
    numbers=np.arange(1,n+1,dtype=np.uint32)
    for i in range(n):
        j=randint(i,n-1)
        numbers[i],numbers[j]=numbers[j],numbers[i]
        yield numbers[i]

num_kv_to_generate = 3000000000 # 3 billion
gen = onthefly(sys.maxint)
dump_file = "/users/aarati_K/realhdd/kv_insert"
f = open(dump_file, 'w')

for i in range(num_kv_to_generate):
    k = next(gen)
    f.write("{}\n".format(k))

f.close()
