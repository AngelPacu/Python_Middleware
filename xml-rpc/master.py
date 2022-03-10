from xmlrpc.server import SimpleXMLRPCServer

## read_csv, apply, columns, groupby, head, isin, items, (GRPC -> max, min).

server = SimpleXMLRPCServer(
    ('localhost', 9000),
    logRequests=True,
    allow_none=True,
)







