import math
import random
import time
import logging

def backoff(i : int):
    if i == 0:
        return
    b = int(math.pow(2, i))
    delay = 0.001 * random.randint(b, 2*b)
    logging.debug('backoff %f', delay)
    time.sleep(delay)
