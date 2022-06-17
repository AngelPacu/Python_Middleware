import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
import threading

## read_csv, apply, columns, groupby, head, isin, items, (GRPC -> max, min).


worker_list = list()


def run_server():
    server = SimpleXMLRPCServer(
        ('localhost', 9000),
        logRequests=True,
        allow_none=True
    )

    def register_worker(port):
        worker_list.append(port)
        #for worker in worker_list:
        #    worker.worker_list = worker_list

    def list_workers():
        return worker_list if worker_list else None

    def assign_worker():
        return "Worker assigned" if worker_list else "No workers available"

    def read_csv(filepath):
        # compare = dd.read_csv(filepath)
        for worker in worker_list:
            w_proxy = xmlrpc.client.ServerProxy('http://localhost:' + worker)
            if result is None:
                result = w_proxy.read_csv(filepath)
                print("Read_csv worker " + worker + ": " + result)
            elif result != w_proxy.read_csv(filepath):
                result = "Consistency error between nodes"
                print("Read_csv worker " + worker + ": " + result)
                break
        return result

    # numpy.sum can be replaced.
    def apply(filepath):
        result = None
        for worker in worker_list:
            w_proxy = xmlrpc.client.ServerProxy('http://localhost:' + worker)
            if result is None:
                result = w_proxy.apply(filepath)
                print("Apply worker " + worker + ": " + result)
            elif result != w_proxy.apply(filepath):
                result = "Consistency error between nodes"
                print("Apply worker " + worker + ": " + result)
        return result

    def columns(filepath):
        result = None
        for worker in worker_list:
            w_proxy = xmlrpc.client.ServerProxy('http://localhost:' + worker)
            if result is None:
                result = w_proxy.columns(filepath)
                print("Columns worker " + worker + ": " + result)
            elif result != w_proxy.columns(filepath):
                result = "Consistency error between nodes"
                print("Columns worker " + worker + ": " + result)
        return result

    # Averages with matching numbers, if there are 2 matching values, it will average the entire row. EX: LATD. MEAN
    # can replaced.
    def groupby(filepath, by):
        result = None
        for worker in worker_list:
            w_proxy = xmlrpc.client.ServerProxy('http://localhost:' + worker)
            if result is None:
                result = w_proxy.groupby(filepath)
                print("Groupby worker " + worker + ": " + result)
            elif result != w_proxy.groupby(filepath):
                result = "Consistency error between nodes"
                print("Groupby worker " + worker + ": " + result)
        return result

    # Return a N elements (DEFAULT N=5)
    def head(filepath, num=5):
        result = None
        for worker in worker_list:
            w_proxy = xmlrpc.client.ServerProxy('http://localhost:' + worker)
            if result is None:
                result = w_proxy.head(filepath)
                print("Head worker " + worker + ": " + result)
            elif result != w_proxy.head(filepath):
                result = "Consistency error between nodes"
                print("Head worker " + worker + ": " + result)
        return result

        # Test if cells contain values

    def isin(filepath, values):
        result = None
        for worker in worker_list:
            w_proxy = xmlrpc.client.ServerProxy('http://localhost:' + worker)
            if result is None:
                result = w_proxy.isin(filepath)
                print("Isin worker " + worker + ": " + result)
            elif result != w_proxy.isin(filepath):
                result = "Consistency error between nodes"
                print("Isin worker " + worker + ": " + result)
        return result

        # Iterate function.

    def items(filepath):
        result = None
        for worker in worker_list:
            w_proxy = xmlrpc.client.ServerProxy('http://localhost:' + worker)
            if result is None:
                result = w_proxy.items(filepath)
                print("Items worker " + worker + ": " + result)
            elif result != w_proxy.items(filepath):
                result = "Consistency error between nodes"
                print("items worker " + worker + ": " + result)
        return result
        # Return the maximum of the values

    def maximum(filepath):
        result = None
        for worker in worker_list:
            w_proxy = xmlrpc.client.ServerProxy('http://localhost:' + worker)
            if result is None:
                result = w_proxy.maximum(filepath)
                print("Maximum worker " + worker + ": " + result)
            elif result != w_proxy.maximum(filepath):
                result = "Consistency error between nodes"
                print("Maximum worker " + worker + ": " + result)
        return result

    # Return the minimum of the values

    def minimum(filepath):
        result = None
        for worker in worker_list:
            w_proxy = xmlrpc.client.ServerProxy('http://localhost:' + worker)
            if result is None:
                result = w_proxy.minimum(filepath)
                print("Minimum worker " + worker + ": " + result)
            elif result != w_proxy.minimum(filepath):
                result = "Consistency error between nodes"
                print("Minimum worker " + worker + ": " + result)
        return result

    def check():
        return True

    server.register_function(check, "check")
    server.register_function(read_csv, "read_csv")
    server.register_function(apply, "apply")
    server.register_function(columns, "columns")
    server.register_function(groupby, "groupby")
    server.register_function(head, "head")
    server.register_function(isin, "isin")
    server.register_function(items, "items")
    server.register_function(maximum, "maximum")
    server.register_function(minimum, "minimum")
    server.register_function(register_worker, "register_worker")
    server.register_function(list_workers, "list_workers")
    server.register_function(assign_worker, "assign_worker")

    # To run the server
    try:
        print('Use ctrl+C to exit')
        server.serve_forever()
    except KeyboardInterrupt:
        print("Exit...")
