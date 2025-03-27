from dataclasses import dataclass
from typing import Optional
from mpipe import UnorderedWorker

from plugins.trucklink_models import _DRIVERS_ZSET, _KEY_SEP, DriverID, _encode_driver_id


class WorkerRedisHistoryRemove(UnorderedWorker):
    def __init__(self, **kwargs):
        super().__init__()
        self.redisConn = kwargs['redis_connection']
        


    def doTask(self, value):
        flat_drivers = {k: v for chunk in value for k, v in chunk.items()}
        for driverId, driverData in flat_drivers.items():

            sorted_timestamps = sorted(driverData.keys())

            self.redisConn.zremrangebyscore(
                key=_encode_driver_id(driverId),
                min=sorted_timestamps[0].timestamp(),
                max=sorted_timestamps[-1].timestamp())

def register() -> UnorderedWorker:
    return WorkerRedisHistoryRemove

if __name__ == "__main__":
    from run_standalone import run_standalone
    run_standalone(WorkerRedisHistoryRemove)