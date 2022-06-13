import sys
import time

import redis
import pandas as dd
from xmlrpc.server import SimpleXMLRPCServer

import numpy

def run_server(port):
    server_worker = SimpleXMLRPCServer(
        ('localhost', port),
        allow_none=True,
        logRequests=True,
    )

    server_master = redis.from_url('redis://localhost:6379', db=0)
    worker_name = "localhost"+str(port)
    server_master.rpush("workers", worker_name)
    server_master.set("port",port)
    openDF = dict()

    def read_csv(filepath):
        openDF[filepath] = dd.read_csv(filepath)
        print(openDF[filepath])
        return "CSV read"

    # numpy.sum can be replaced.
    def apply(filepath):
        return str(openDF.get(filepath).apply(numpy.sum))

    def columns(filepath):
        return str(openDF.get(filepath).columns)

    # Averages with matching numbers, if there are 2 matching values, it will average the entire row. EX: LATD. MEAN
    # can replaced.
    def groupby(filepath, by):
        return str(openDF.get(filepath).groupby(openDF[filepath][by]).mean())

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
        return str(openDF.get(filepath).max())

        # Return the minimum of the values

    def minimum(filepath):
        return str(openDF.get(filepath).min(axis=1))

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


run_server(int(sys.argv[1]))
