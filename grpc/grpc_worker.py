import sys
from concurrent import futures
import time
import grpc

import master_pb2
import master_pb2_grpc
import worker
import worker_pb2
import worker_pb2_grpc


def run_server(port):

    class Worker_GrpcServicer(worker_pb2_grpc.WorkerServiceServicer):
        def read_csv(self, request, context):
            response = worker_pb2.Result()
            response.result = worker.read_csv(request.filepath)
            return response

        def maximum(self, request, context):
            response = worker_pb2.Result()
            response.result = worker.maximum(request.filepath, request.num)
            return response

        def minimum(self, request, context):
            response = worker_pb2.Result()
            response.result = worker.minimum(request.filepath, request.num)
            return response



    worker_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # use the generated function `add_WorkerServiceServicer_to_server`
    # to add the defined class to the server
    worker_pb2_grpc.add_WorkerServiceServicer_to_server(Worker_GrpcServicer(), worker_server)

    # We establish Worker Sesion with Master and add Worker.
    master_server = master_pb2_grpc.MasterServiceStub(grpc.insecure_channel('localhost:7777'))
    worker_server.add_insecure_port('[::]:' + str(port))
    master_server.register_worker(master_pb2.Worker(port=port))


    worker_server.start()
    print('Starting server. Listening on port: '+str(port))

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        worker_server.stop(0)


run_server(int(sys.argv[1]))






