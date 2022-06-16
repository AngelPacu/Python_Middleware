import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
from pandas import dd
## read_csv, apply, columns, groupby, head, isin, items, (GRPC -> max, min).


server = SimpleXMLRPCServer(
    ('localhost', 9000),
    logRequests=True,
    allow_none=True
)

worker_list = list()


def register_worker(port):
    worker_list.append(xmlrpc.client.ServerProxy('http://localhost:'+port))
    for worker in worker_list:
        worker.worker_list = worker_list

def list_workers():
    return worker_list if worker_list else None


def assign_worker():
    worker_port = worker_list[0] if worker_list else None
    # worker_list.pop(0)
    return "Worker assigned"


def read_csv(filepath):

    for worker in worker_list:
        df = worker.openDF[filepath] = dd.read_csv(filepath)


    return "CSV read"


# numpy.sum can be replaced.
def apply(filepath):
    return str(openDF.get(filepath).apply(numpy.sum))


def columns(filepath):
    return str(openDF.get(filepath).columns)


# Averages with matching numbers, if there are 2 matching values, it will average the entire row. EX: LATD. MEAN
# can replaced.
def groupby(filepath, by):
    return str(openDF.get(filepath).groupby(openDF.get(filepath)[by]).mean())


# Return a N elements (DEFAULT N=5)
def head(filepath, num=5):
    return str(openDF.get(filepath).head(num))

    # Test if cells contain values


def isin(filepath, values):
    return str(openDF.get(filepath).isin(values))

    # Iterate function.


def items(filepath):
    df_str = ''
    for label, value in openDF[filepath].items():
        df_str += f'label: {label}\n'
        df_str += f'content:\n {value}\n'

    return str(df_str)
    # Return the maximum of the values


def maximum(filepath):
    return str(openDF.get(filepath).max(numeric_only='True'))


# Return the minimum of the values

def minimum(filepath):
    return str(openDF.get(filepath).min(numeric_only='True'))


server_worker.register_function(read_csv, "read_csv")
server_worker.register_function(apply, "apply")
server_worker.register_function(columns, "columns")
server_worker.register_function(groupby, "groupby")
server_worker.register_function(head, "head")
server_worker.register_function(isin, "isin")
server_worker.register_function(items, "items")
server_worker.register_function(maximum, "maximum")
server_worker.register_function(minimum, "minimum")

server.register_function(register_worker, "register_worker")
server.register_function(list_workers, "list_workers")
server.register_function(assign_worker, "assign_worker")

# To run the server
try:
    print('Use ctrl+C to exit')
    server.serve_forever()
except KeyboardInterrupt:
    print("Exit...")
