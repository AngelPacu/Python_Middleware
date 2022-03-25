import xmlrpc.client

master_server = xmlrpc.client.ServerProxy('http://localhost:9000', allow_none=True)
print(master_server.list_workers())

ruta = "http://localhost:"+str(master_server.assign_worker())
worker_server = xmlrpc.client.ServerProxy(ruta, allow_none=True)
worker_server.read_csv('../dataFiles/cities.csv')

