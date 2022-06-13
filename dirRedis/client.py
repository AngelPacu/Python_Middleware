import timeit
import xmlrpc.client
import redis

server_master = redis.from_url('redis://localhost:6379', db=0)
mydf = "../dataFiles/cities.csv"

def register_workers():
    w_list = list()
    for i in range (server_master.llen("workers")):
        w_list.append(server_master.lindex("workers", i).decode("utf-8"))

    return w_list

def assign_worker():
    worker_port = worker_list[0] if worker_list else None
    print(worker_list)
    # worker_list.pop(0)
    return worker_port


worker_list = register_workers()

worker = assign_worker()

port = server_master.get("port").decode("utf-8")
ruta = "http://localhost:"+port
worker_server = xmlrpc.client.ServerProxy(ruta)

worker_server.read_csv(mydf)
print(worker_server.minimum(mydf))
print(worker_server.maximum(mydf))

result_read = timeit.timeit(stmt='worker_server.read_csv(mydf)', globals=globals(), number=1)
result_min = timeit.timeit(stmt='worker_server.minimum(mydf)', globals=globals(), number=1)
result_max = timeit.timeit(stmt='worker_server.maximum(mydf)', globals=globals(), number=1)


print(result_read)
print(result_max)
print(result_min)

