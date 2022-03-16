import encodings

import redis

pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
r = redis.Redis(connection_pool=pool)
pool.get_encoder()


class Worker:
    def __init__(self, name):
        self.name = name
        myport = int.from_bytes(name.encode(), 'little')
        mypool = redis.ConnectionPool(host='localhost', port=myport, db=0)
        r.set(name, str(mypool))
        redis.utils.

    def
worker = Worker("ismi")
workernoob = Worker("pacuardo")