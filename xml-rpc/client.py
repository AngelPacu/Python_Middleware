import xmlrpc.client

server = xmlrpc.client.ServerProxy('http://localhost:9000')
server.read_csv('dataFiles/cities.csv')
print(server.head())
