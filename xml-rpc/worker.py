import xmlrpc.client
import dask.dataframe as dd
from xmlrpc.server import SimpleXMLRPCServer

serverWorker = SimpleXMLRPCServer(
    ('localhost', 1000),
    logRequests=True,
    allow_none=True,
)
serverClient = xmlrpc.client.ServerProxy('http://localhost:9000')
df = dd.DataFrame


def read_csv(filepath, **params):
    df.read_csv(filepath, params)


def apply(**params):
    df.apply(params)


def columns():
    return df.g


def groupby(by, **params):
    return df.groupby(by, params)


# Return a first element.
def head():
    return df.head()


# Pa comprobar si las celdas contienen el "value"
def isin(values):
    return df.isin(values)


# Iterate function.
def items():
    df.items()


# Return the maximum of the values
def max():
    return df.max()


# Return the minimum of the values
def min():
    return df.min()


serverWorker.register_function(read_csv, "read_csv")
serverWorker.register_function(apply, "apply")
serverWorker.register_function(columns, "columns")
serverWorker.register_function(head, "read_csv")
serverWorker.register_function(isin, "isin")
serverWorker.register_function(max, "max")
serverWorker.register_function(min, "min")

#Para runear el server
try:
    print('Usar Control-C pa salir')
    serverWorker.serve_forever()
except KeyboardInterrupt:
    print("Exit...")


