import xmlrpc.client
import timeit
from multiprocessing import Pool
from time import sleep

import worker2

mydf = "../dataFiles/cities.csv"

worker_server = xmlrpc.client.ServerProxy('http://localhost:9000')

while worker_server.assign_worker() != "Worker assigned":
    sleep(2)

result_read = timeit.timeit(stmt='worker_server.read_csv(mydf)', globals=globals(), number=1)
result_max = timeit.timeit(stmt='worker_server.minimum(mydf)', globals=globals(), number=1)
result_min = timeit.timeit(stmt='worker_server.maximum(mydf)', globals=globals(), number=1)
result_isin = timeit.timeit(stmt='worker_server.isin(mydf, [0,50])', globals=globals(), number=1)
result_col = timeit.timeit(stmt='worker_server.columns(mydf)', globals=globals(), number=1)
result_head = timeit.timeit(stmt='worker_server.head(mydf)', globals=globals(), number=1)
result_by = timeit.timeit(stmt='worker_server.groupby(mydf, "LatD")', globals=globals(), number=1)
#print(result_read)
#print(result_max)
#print(result_min)
#print(result_read)
#print(result_isin)
#print(result_col)
#print(result_head)
#print(result_by)


### TEST FUNCTIONS OK  // TODAS OK
#print("\nMin:"+worker_server.minimum(mydf))
#print("\nMax:"+worker_server.maximum(mydf))
#print("\nIsin:"+worker_server.isin(mydf, [0,50]))
#print("\nColumns:"+worker_server.columns(mydf))
#print("\nHead:"+worker_server.head(mydf))
print("\nGroupby:"+worker_server.groupby(mydf, "LatD"))
#print(worker_server.apply(mydf))
#print (worker_server.items(mydf))







