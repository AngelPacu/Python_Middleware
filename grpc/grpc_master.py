from concurrent import futures
import time
import grpc

import master
import master_pb2
import master_pb2_grpc

class Master_GrpcServicer(master_pb2_grpc.MasterServiceServicer):

    def register_worker(self, port, context):
        master.register_worker(port.port)
        return master_pb2.list()

    def list_workers(self, request, context):
        response = master_pb2.WorkerList()
        response.workerList[:] = master.list_workers()
        return response

    def assign_worker(self, request, context):
        response = master_pb2.Worker()
        response.port = master.assign_worker()
        return response


# create a gRPC-distributed-dataframe server
server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

# use the generated function `add_MasterServiceServicer_to_server`
# to add the defined class to the server
master_pb2_grpc.add_MasterServiceServicer_to_server(Master_GrpcServicer(), server)

# listen on port 7777
print('Starting server. Listening on port 7777.')
server.add_insecure_port('[::]:7777')
server.start()

# since server.start() will not block,
# a sleep-loop is added to keep alive
try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    server.stop(0)
