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
    worker_name = worker_list[0] if worker_list else None
    print(worker_list)
    print(worker_name)
    # worker_list.pop(0)
    port = server_master.get(worker_name).decode("utf-8")
    ruta = "http://localhost:" + port
    worker = xmlrpc.client.ServerProxy(ruta)
    return worker


worker_list = register_workers()
worker_server = assign_worker()


### TEST FUNCTIONS ###
worker_server.read_csv(mydf)
print(worker_server.minimum(mydf))
print(worker_server.maximum(mydf))
result_read = timeit.timeit(stmt='worker_server.read_csv(mydf)', globals=globals(), number=1)
result_min = timeit.timeit(stmt='worker_server.minimum(mydf)', globals=globals(), number=1)
result_max = timeit.timeit(stmt='worker_server.maximum(mydf)', globals=globals(), number=1)
print(result_read)
print(result_max)
print(result_min)

