import xmlrpc.client
import timeit
from multiprocessing import Pool
from time import sleep


mydf = "../dataFiles/cities.csv"

worker_server = xmlrpc.client.ServerProxy('http://localhost:9000')

print(worker_server.list_workers)

while worker_server.assign_worker() != "Worker assigned":
    sleep(2)



### TEST FUNCTIONS OK  // TODAS OK
print("\nMin:"+worker_server.read_csv(mydf))
print("\nMin:"+worker_server.minimum(mydf))
#print("\nMax:"+worker_server.maximum(mydf))
#print("\nIsin:"+worker_server.isin(mydf, [0,50]))
#print("\nColumns:"+worker_server.columns(mydf))
#print("\nHead:"+worker_server.head(mydf))
print("\nGroupby:"+worker_server.groupby(mydf, "LatD"))
#print(worker_server.apply(mydf))
#print (worker_server.items(mydf))







