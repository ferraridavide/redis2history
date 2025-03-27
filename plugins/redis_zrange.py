from datetime import datetime, timezone
import itertools
from typing import Iterable
import numpy as np
from redis import Redis
from mpipe import UnorderedWorker


def _chunks(iterable: Iterable, size: int):
    # See: https://stackoverflow.com/a/24527424
    iterator = iter(iterable)
    for first in iterator:
        yield itertools.chain([first], itertools.islice(iterator, size - 1))

class WorkerRedisZRange(UnorderedWorker):
    def __init__(self, **kwargs):
        super().__init__()
        self.redisConn: Redis = kwargs['redis_connection']
        self.r_start: str = kwargs['args']['start']
        self.r_end: str =   kwargs['args']['end']

    def task(self, key):
        tz = timezone.utc
        values = self.redisConn.zrangebyscore(
                key,
                min=self.r_start,
                max=self.r_end,
                withscores=True)
        self.putResult([key, {datetime.fromtimestamp(int(value[1]),tz):value[0] for value in values}])

    def doTask(self, key):
        if isinstance(key, (list, tuple, np.ndarray)):
            for v in key:
                self.task(v)
        else:
            self.task(key)
        

def register() -> UnorderedWorker:
    return WorkerRedisZRange

if __name__ == "__main__":
    from run_standalone import run_standalone
    run_standalone(WorkerRedisZRange, iterate_list=True)