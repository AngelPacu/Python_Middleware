import xmlrpc.client

master_server = xmlrpc.client.ServerProxy('http://localhost:9000')
print(master_server.list_workers())

ruta = "http://localhost:" + str(master_server.assign_worker())
worker_server = xmlrpc.client.ServerProxy(ruta)

mydf = '../dataFiles/cities.csv'
worker_server.read_csv(mydf)

### TEST FUNCTIONS OK

#print(worker_server.minimum(mydf))
#print(worker_server.maximum(mydf))
#print(worker_server.isin(mydf, [0,50]))
#print(worker_server.columns(mydf))
#print(worker_server.head(mydf))
#print(worker_server.max(mydf))
print(worker_server.items(mydf))

#print(worker_server.apply(mydf, print))
print(worker_server.groupby(mydf, "City"))



