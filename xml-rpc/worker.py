import sys
import xmlrpc.client
import pandas as dd
from xmlrpc.server import SimpleXMLRPCServer


class Worker:

    def __init__(self, port):
        self.server_worker = SimpleXMLRPCServer(
            ('localhost', port),
            logRequests=True,
            allow_none=True
        )
        server_master = xmlrpc.client.ServerProxy('http://localhost:9000')
        server_master.register_worker(port)
        self.df = dd.DataFrame
        self.run_server()

    def read_csv(self, filepath):
        self.df = dd.read_csv(filepath)

    def apply(self, **params):
        self.df.apply(params)

    def columns(self):
        return str(self.df.columns)

    def groupby(self, by, **params):
        return self.df.groupby(by, params)

    # Return a N elements (DEFAULT N=5)
    def head(self, num=5):
        return str(self.df.head(num))

    # Pa comprobar si las celdas contienen el "value"
    def isin(self, values):
        return str(self.df.isin(values))

    # Iterate function.
    def items(self):
        self.df.items()

    # Return the maximum of the values
    def max(self):
        return str(self.df.max)

    # Return the minimum of the values
    def min(self):
        return str(self.df.min())

    def run_server(self):
        self.server_worker.register_function(self.read_csv, "read_csv")
        self.server_worker.register_function(self.apply, "apply")
        self.server_worker.register_function(self.columns, "columns")
        self.server_worker.register_function(self.head, "head")
        self.server_worker.register_function(self.isin, "isin")
        self.server_worker.register_function(self.max, "max")
        self.server_worker.register_function(self.min, "min")

        # To run the server
        try:
            print('Worker with port ' + (sys.argv[1]) + ' ready for battle...')
            self.server_worker.serve_forever()
        except KeyboardInterrupt:
            print("Exit...")


Worker(sys.argv[1])
