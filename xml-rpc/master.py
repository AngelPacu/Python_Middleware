from xmlrpc.server import SimpleXMLRPCServer

## read_csv, apply, columns, groupby, head, isin, items, (GRPC -> max, min).


server = SimpleXMLRPCServer(
    ('localhost', 9000),
    logRequests=True,
    allow_none=True
)

worker_list = list()

def register_worker(port):
    worker_list.append(port)


def list_workers():
    return worker_list if worker_list else None


def assign_worker():
    worker_port = worker_list[0] if worker_list else None
    #worker_list.pop(0)
    return worker_port


server.register_function(register_worker, "register_worker")
server.register_function(list_workers, "list_workers")
server.register_function(assign_worker, "assign_worker")

# To run the server
try:
    print('Use ctrl+C to exit')
    server.serve_forever()
except KeyboardInterrupt:
    print("Exit...")
