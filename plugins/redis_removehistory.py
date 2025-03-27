from dataclasses import dataclass
from typing import Optional
from mpipe import UnorderedWorker

from plugins.trucklink_models import _KEY_SEP, _VEHICLES_ZSET, VehicleID, _encode_vehicle_id

class WorkerRedisHistoryRemove(UnorderedWorker):
    def __init__(self, **kwargs):
        super().__init__()
        self.redisConn = kwargs['redis_connection']

    def doTask(self, value):
        for vehicle in value:
            vehicleId = vehicle[0]
            vehicleData = vehicle[1]

            sorted_timestamps = sorted(vehicleData.keys())

            self.redisConn.zremrangebyscore(
                key=_encode_vehicle_id(vehicleId),
                min=sorted_timestamps[0].timestamp(),
                max=sorted_timestamps[-1].timestamp())

def register() -> UnorderedWorker:
    return WorkerRedisHistoryRemove

if __name__ == "__main__":
    from run_standalone import run_standalone
    run_standalone(WorkerRedisHistoryRemove)