
import math
import random
import time

def backoff(i : int):
    b = int(math.exp2(i))
    time.sleep(0.1 * random.randint(b, 2*b))
