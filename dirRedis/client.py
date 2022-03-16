import random

import redis

pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
r = redis.Redis(connection_pool=pool)

availableWorkers = r.keys()
chosen = availableWorkers[random.randint(0, len(availableWorkers))].decode()
workerPool = r.get(chosen).decode()
workerPool = redis.ConnectionPool(workerPool)
# r.delete(chosen)

myworker = redis.Redis(connection_pool=workerPool)
