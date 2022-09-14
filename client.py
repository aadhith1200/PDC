import sys
import zmq

from print_seq import seqlog
from logger import log
import json
import random
import time

SERVER_ENDPOINT = "tcp://localhost:5555"

def generate_random_matrix(n):

    matA = [[random.randrange(0, 5) for x in range(n)] for y in range(n)]
    matB = [[random.randrange(0, 5) for x in range(n)] for y in range(n)]
    return matA, matB

def testSerial(n, A, B):

    result = [[0 for j in range(n)]for i in range(n)]
 
    for i in range(len(A)):
        for j in range(len(B[0])):
            for k in range(len(B)):
                result[i][j] += A[i][k] * B[k][j]

    return result

def request():
    n = 3
    context = zmq.Context()
    client = context.socket(zmq.REQ)
    client.connect(SERVER_ENDPOINT)
    mA, mB = generate_random_matrix(n)
    # mA = [[4, 3], [0, 0]]
    # mB = [[1, 3], [4, 0]]
    toBeSent = {"m1":mA, "m2":mB}
    res = testSerial(n, mA,mB)
    # log.info(res)
    startTime = time.perf_counter()
    client.send_string(json.dumps(toBeSent))
    seqlog(["client","queue"],payload="Input data sent")
    while True:
        if (client.poll() & zmq.POLLIN) != 0:
            reply = client.recv_multipart()
            seqlog(['queue', 'client'], payload=reply[-1])
            if(json.loads(reply[-1].decode())["output"] == res):
                log.info("Correct output")
            else:
                log.info("Incorrect output")
            return reply


class ConnectionError(Exception):
    pass


if __name__ == '__main__':
    request()
    while(1):
        pass