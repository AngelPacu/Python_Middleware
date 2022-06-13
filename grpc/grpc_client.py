import grpc

import master_pb2
import master_pb2_grpc
import worker_pb2_grpc
import worker_pb2
import timeit

master_server = master_pb2_grpc.MasterServiceStub(grpc.insecure_channel('localhost:7777'))
ruta = master_server.list_workers(master_pb2.list()).workerList
print(ruta)
worker_test = master_server.assign_worker(master_pb2.list()).port
print(worker_test)
worker_server = worker_pb2_grpc.WorkerServiceStub(grpc.insecure_channel(('localhost:'+str(worker_test))))


## TEST FUNCTIONS ##
mydf = "../dataFiles/cities.csv"
print(worker_server.read_csv(worker_pb2.Filepath(filepath=mydf)))
max1 = str(worker_server.maximum(worker_pb2.Filepath(filepath=mydf, num=1))).replace('\\n', '\n')
min1 = str(worker_server.minimum(worker_pb2.Filepath(filepath=mydf, num=1))).replace('\\n', '\n')

print(max1)
print(min1)

result_read = timeit.timeit(stmt='worker_server.read_csv(worker_pb2.Filepath(filepath=mydf))', globals=globals(), number=1)
result_max = timeit.timeit(stmt='worker_server.maximum(worker_pb2.Filepath(filepath=mydf, num=1))', globals=globals(), number=1)
result_min = timeit.timeit(stmt='worker_server.minimum(worker_pb2.Filepath(filepath=mydf, num=1))', globals=globals(), number=1)


print(result_read)
print(result_max)
print(result_min)


