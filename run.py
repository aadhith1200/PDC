import multiprocessing as mp
import sys
import time
import threading

from logger import log
from worker import worker
from queue import queue
from client import request, ConnectionError


def make_requests():
    try:
        request()
    except ConnectionError as err:
        log.error(err)
        sys.exit(1)

if __name__ == '__main__':
    mp.Process(target=queue, daemon=True).start()
    for i in range(6):
        mp.Process(target=worker, daemon=True).start()
    mp.Process(target=make_requests, daemon=True).start()
    while True:
        time.sleep(1)
