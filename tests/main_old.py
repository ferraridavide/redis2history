from multiprocessing import freeze_support
from multiprocessing.managers import BaseManager, BaseProxy, Namespace
from redis import Redis
import loader
import factory
import analyzer
import pickle

from load_config import load_config
from mpipe import Pipeline
from clients import RedisClient


class CustomManager(BaseManager):
    pass

if __name__ == '__main__':
    freeze_support()

    CustomManager.register('redis', Redis)
    with CustomManager() as manager:
        shared_redis = manager.redis(host='localhost', port=6379, ssl=False, decode_responses=True)
        

        config = load_config('./config.json')
        redis = RedisClient(config.redisSettings)

        analyzer.analize_config(config.transforms)
        loader.load_transforms(config.transforms, redisConn=shared_redis, globalArgs=config.globalArgs) 

        for stage in config.transforms:
            if stage.source:
                factory.get(stage.source).link(factory.get(stage.id))



        pipe = Pipeline(factory.get("redisVehiclesKeys"))

        pipe.put("")

        input()


