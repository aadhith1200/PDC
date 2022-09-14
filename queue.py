from print_seq import seqlog
from logger import log
import zmq
import json

READY = b"\x01"
WORKER_ADDR = "tcp://*:5556"
CLIENT_ADDR = "tcp://*:5555"
client_data = {}
work_list = []

def createResMatrix(client_id, n):
    client_data[client_id] = [[["" for i in range(n)] for j in range(n)],n*n]

def updateResMatrix(client_id, index, val):
    client_data[client_id][0][index[0]][index[1]] = val
    client_data[client_id][1] -= 1
    if(client_data[client_id][1] == 0):
        return True
    else:
        return False

def divideTasks(inp_dict, client_id):

    ver = []
    for i in range(len(inp_dict["m2"])):
        temp = []
        for j in range(len(inp_dict["m2"])):
            temp.append(inp_dict["m2"][j][i])
        ver.append(temp)

    for i in range(len(inp_dict["m1"])):
        for j in range(len(ver)):
            work_list.append([{"arr1":inp_dict["m1"][i], "arr2":ver[j], "index":[i,j]},client_id])

    # for k in range(len(inp_dict["m1"])):
    #     for i in range(len(inp_dict["m2"])):
    #         temp = []
    #         for j in range(len(inp_dict["m2"])):
    #             temp.append(inp_dict["m2"][j][i])
    #         work_list.append({"arr1":inp_dict["m1"][k], "arr2":temp, "index":10*k+i})



def queue():
    context = zmq.Context(1)
    frontend = context.socket(zmq.ROUTER)
    backend = context.socket(zmq.ROUTER)
    frontend.bind(CLIENT_ADDR)
    backend.bind(WORKER_ADDR)
    poll_workers = zmq.Poller()
    poll_workers.register(backend, zmq.POLLIN)
    poll_both = zmq.Poller()
    poll_both.register(frontend, zmq.POLLIN)
    poll_both.register(backend, zmq.POLLIN)
    workers = []
    log.info("Queue has started")
    while True:
        if len(workers) > 0:
            socks = dict(poll_both.poll())

        else:
            socks = dict(poll_workers.poll())

        if socks.get(backend) == zmq.POLLIN:
            msg = backend.recv_multipart()
            if not msg:
                continue
            address = msg[0]
            workers.append(address)
            reply = msg[2:]
            if reply[0] != READY:
                seqlog(['workers', 'queue'], payload=reply[-1], rtl=False)
                #log.info(reply+["broker"])
                res = json.loads(reply[-1].decode())
                if(updateResMatrix(reply[0],res["index"], res["val"])):
                    final_res = {"output":client_data[reply[0]][0]}
                    frontend.send_multipart([reply[0],b'',json.dumps(final_res).encode()])

        if socks.get(frontend) == zmq.POLLIN:
            msg = frontend.recv_multipart()
            d = json.loads(msg[-1])
            request = [workers.pop(0), ''.encode()] + msg
            seqlog(['queue', 'client'], "Input Data received", rtl=True)
            divideTasks(d, msg[0])
            createResMatrix(msg[0],len(d["m1"]))
            #log.info(work_list)


        for worker in workers:
            if(len(work_list) > 0):
                task = work_list.pop()
                backend.send_multipart([worker, b"", task[1], b"", json.dumps(task[0]).encode()])
                seqlog(['workers','queue'],task,rtl=True)
                workers.remove(worker)
