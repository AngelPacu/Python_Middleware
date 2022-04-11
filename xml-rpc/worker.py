import xmlrpc.client
import pandas as dd
from xmlrpc.server import SimpleXMLRPCServer


def run_worker(port):
    print(port)
    server_worker = SimpleXMLRPCServer(
        ('localhost', port),
        logRequests=True,
    )

    server_master = xmlrpc.client.ServerProxy('http://localhost:9000')
    server_master.register_worker(str(port))
    openDF = dict()

    def read_csv(filepath):
        openDF[filepath] = dd.read_csv(filepath)
        return "CSV read"

    def apply(filepath, function):
        openDF.get(filepath).apply(func=function)
        return "Function Applied"

    def columns(filepath):
        return str(openDF.get(filepath).columns)

    def groupby(filepath, by):
        return openDF.get(filepath).keys()

        # Return a N elements (DEFAULT N=5)

    def head(filepath, num=5):
        return str(openDF.get(filepath).head(num))

        # Test if cells contain values

    def isin(filepath, values):
        return str(openDF.get(filepath).isin(values))

        # Iterate function.

    def items(filepath):
        items = openDF.get(filepath).items()
        print(items)
        return xd
        # Return the maximum of the values

    def maximum(filepath):
        return str(openDF.get(filepath).max())

        # Return the minimum of the values

    def minimum(filepath):
        return str(openDF.get(filepath).min())

    server_worker.register_function(read_csv, "read_csv")
    server_worker.register_function(apply, "apply")
    server_worker.register_function(columns, "columns")
    server_worker.register_function(groupby, "groupby")
    server_worker.register_function(head, "head")
    server_worker.register_function(isin, "isin")
    server_worker.register_function(items, "items")
    server_worker.register_function(maximum, "maximum")
    server_worker.register_function(minimum, "minimum")

    # To run the server
    try:
        print('Worker with port ' + str(port) + ' ready for battle...')
        server_worker.serve_forever()
    except KeyboardInterrupt:
        print("Exit...")
