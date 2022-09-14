import time
import uuid
import zmq
import json

from print_seq import seqlog
from logger import log

READY = "\x01"
WORKER_ADDR = "tcp://localhost:5556"


def worker():
    context = zmq.Context(1)
    worker = context.socket(zmq.REQ)
    identity = str(uuid.uuid4())
    worker.setsockopt_string(zmq.IDENTITY, identity)
    worker.connect(WORKER_ADDR)
    log.info(f"({identity}) worker ready")
    worker.send_string(READY)
    request_count = 0
    while True:
        msg = worker.recv_multipart()
        #log.info([identity] + msg)
        comp_val = 0
        msg1 = json.loads(msg[-1].decode())
        arr1 = msg1["arr1"]
        arr2 = msg1["arr2"]
        for i in range(len(arr1)):
            comp_val += arr1[i] * arr2[i]
        #log.info(comp_val)
        toBeSent = {"val":comp_val, "index":msg1["index"]}
        worker.send_multipart([msg[0],b"",json.dumps(toBeSent).encode()])
