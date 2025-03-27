import sys
sys.path.append("/home/ubuntu/siak/mpipe_test")

import types
from redis import Redis
from rediscluster import RedisCluster
from models import RedisSettings






class RedisClient:
    def __init__(self, s:RedisSettings):
        if s.cluster:
            self.connection = RedisCluster(host=s.host, port=s.port, ssl=s.tls, decode_responses=True,
                                      skip_full_coverage_check=True)
        else:
            self.connection = Redis(host=s.host, port=s.port, ssl=s.tls, decode_responses=True)

    def __getattr__(self, name):
            attr = getattr(self.connection, name)
            if callable(attr):
                print("CALLING: " + attr.__name__)
                def wrapper(*args, **kwargs):
                    result = attr(*args, **kwargs)
                    if isinstance(result, types.GeneratorType):
                        return list(result)
                    else:
                        return result
                return wrapper
            else:
                raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
    


rClient = RedisClient(RedisSettings("siak-redis1",6379,False, False))
t = rClient.scan_iter("trucklink:vehicles-hs:*")
print(type(t))
    