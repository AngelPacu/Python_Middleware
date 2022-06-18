import xmlrpc.client
from time import sleep

mydf = "../dataFiles/cities.csv"

worker_server = xmlrpc.client.ServerProxy('http://localhost:9000')
print(worker_server.list_workers())

while worker_server.assign_worker() != "Worker assigned":
    print("No workers avaiilable")
    sleep(2)

### TEST FUNCTIONS OK  // TODAS OK
while True:
    try:
        print("\nWorkers:" + worker_server.list_workers())
        sleep(2)
        print("\nRead:" + worker_server.read_csv(mydf))
        sleep(2)
        print("\nMin:" + worker_server.minimum(mydf))
        sleep(2)
        print("\nMax:" + worker_server.maximum(mydf))
        sleep(2)
        print("\nIsin:" + worker_server.isin(mydf, [0, 50]))
        sleep(2)
        print("\nColumns:" + worker_server.columns(mydf))
        sleep(2)
        print("\nHead:" + worker_server.head(mydf))
        sleep(2)
        print("\nGroupby:" + worker_server.groupby(mydf, "LatD"))
        sleep(2)
        print("\nApply:" + worker_server.apply(mydf))
        sleep(2)
        print("\nItems:" + worker_server.items(mydf))
    except (ConnectionError, TimeoutError) as e:
        print("There was a problem while your operation was being processed, execution will reload shortly")
        continue
    except Exception:
        print("There was an intern error, consistency could not be provided")
        continue
