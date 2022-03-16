import redis
import dirRedis

masterServer = redis.Redis(host='localhost', port=6379, db=0)
print(masterServer.ping())
print(masterServer.client_list())
