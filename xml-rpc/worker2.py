import sys
import xmlrpc.client
import pandas as dd
from xmlrpc.server import SimpleXMLRPCServer
import master
import threading
from time import sleep
import numpy

worker_list = list()
server_master = xmlrpc.client.ServerProxy('http://localhost:9000')


def run_server(port):
    try:
        server_master.register_worker(str(port))

    except ConnectionError:
        master_test = master.run_server
        run_master = threading.Thread(target=master_test, daemon=True)
        run_master.start()
        print("El master será este")
        server_master.register_worker(str(port))

    server_worker = SimpleXMLRPCServer(
        ('localhost', port),
        allow_none=True,
        logRequests=True,
    )

    openDF = dict()

    def read_csv(filepath):
        openDF[filepath] = dd.read_csv(filepath)
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

    def check():
        return True

    server_worker.register_function(read_csv, "read_csv")
    server_worker.register_function(apply, "apply")
    server_worker.register_function(columns, "columns")
    server_worker.register_function(groupby, "groupby")
    server_worker.register_function(head, "head")
    server_worker.register_function(isin, "isin")
    server_worker.register_function(items, "items")
    server_worker.register_function(maximum, "maximum")
    server_worker.register_function(minimum, "minimum")
    server_worker.register_function(check, "check")

    # To run the server
    try:
        print('Worker with port ' + str(port) + ' ready for battle...')
        run_worker = threading.Thread(target=server_worker.serve_forever, daemon=True)
        run_worker.start()
        sleep(2)
        while True:
            print(server_master.list_workers())
            sleep(3)
    except KeyboardInterrupt:
        server_master.remove_worker(port)
        print("Exit...")

    except ConnectionRefusedError:
        run_server(port)


run_server(int(sys.argv[1]))
