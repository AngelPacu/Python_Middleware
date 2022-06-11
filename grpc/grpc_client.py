import grpc

import master_pb2
import master_pb2_grpc
import worker_pb2_grpc
import worker_pb2

import pandas as dd

master_server = master_pb2_grpc.MasterServiceStub(grpc.insecure_channel('localhost:7777'))
ruta = master_server.list_workers(master_pb2.list()).workerList
print(ruta)
worker_test = master_server.assign_worker(master_pb2.list()).port
print(worker_test)
worker_server = worker_pb2_grpc.WorkerServiceStub(grpc.insecure_channel(('localhost:'+str(worker_test))))


mydf = "../dataFiles/cities.csv"
print(worker_server.read_csv(worker_pb2.Filepath(filepath=mydf)))




