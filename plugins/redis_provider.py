import multiprocessing
from multiprocessing.managers import BaseManager
from redis import Redis
from models import RedisSettings
from mpipe import UnorderedWorker
from clients import RedisClient

class CustomManager(BaseManager):
    pass

class WorkerRedisProvider(UnorderedWorker):
    def __init__(self, **kwargs):
        super().__init__()


    def doTask(self, value):
        CustomManager.register('redis', Redis)
        manager = CustomManager()
        manager.start()
        shared_redis = manager.redis(host='localhost', port=6379, ssl=False, decode_responses=True)
        self.putResult(shared_redis)
        self.putResult(None)

def register() -> UnorderedWorker:
    return WorkerRedisProvider