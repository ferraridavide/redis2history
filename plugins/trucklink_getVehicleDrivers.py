from collections import OrderedDict
import datetime
from typing import Optional
from redis import Redis
from mpipe import UnorderedWorker
import numpy as np

from plugins.trucklink_models import DriverID, DriverValues, Position, _encode_driver_id



def _split_values(s: str) -> dict[str, str]:
    # See: https://stackoverflow.com/a/5389547
    tokens = s.split('#')
    keyvals = zip(*[iter(tokens)] * 2)
    return {kv[0]: kv[1] for kv in keyvals}

def _errval(d: dict[str, str], root: str) -> Optional[str]:
    return d.pop(root + '.value', None) if not d.pop(root + '.error', None) else None

def _decode_driver_values(d: dict[str, str], vId, vValue, slot: int) -> DriverValues:
    return DriverValues(
        position=vValue.position,         # VEICOLO
        tacho_slot=str(slot),           # SLOT
        vin=vId.vin,                    # VEICOLO
        plate=vValue.plate,               # VEICOLO
        tco_speed=vValue.tco_speed,       # VEICOLO
        status=vValue.driver1status if slot == 1 else vValue.driver2status, # VEICOLO
        break_time=_errval(d, 'day.breakTime'),                     # DRIVER
        cont_driv_time=_errval(d, 'day.contDrivingTime'),           # DRIVER
        curr_acti_time=_errval(d, 'day.currActivityTime'),          # DRIVER
        tota_driv_time=_errval(d, 'day.totaDrivingTime'),           # DRIVER
        out_of_scope=_errval(d, 'day.outOfScope'),                  # DRIVER
        multi_manning=_errval(d, 'day.multiManning'),               # DRIVER
        left_10hour_drives=_errval(d, 'day.left10HourDrives'),      # DRIVER
        left_reduced_rests=_errval(d, 'day.leftReducedRests'),      # DRIVER
        vehicle_distance=_errval(d, 'vehicleDistance'),             # DRIVER
        tachoTime=_errval(d, 'tachoTime'),             # DRIVER
        prevStatus=_errval(d, 'prevStatus'),             # DRIVER
    )


class WorkerTrucklinkGetVehicleDrivers(UnorderedWorker):
    def __init__(self, **kwargs): # --> 3. I workers ricevono la sharedCache
        super().__init__()
        self.driverCache = {} #driverCache
        self.redisConn: Redis = kwargs['redis_connection']
        self.r_start: str = kwargs['args']['start']
        self.r_end: str =   kwargs['args']['end']
        self.drivers: dict[DriverID, dict[datetime.datetime, DriverValues]] = {}

    # --> Viene overridden assemble per instanziare una sharedCache



    def getCards(self, vId, vValue):
        if card := vValue.driver1card:
            yield DriverID(card=card, account=vId.account)      
        if card := vValue.driver2card:
            yield DriverID(card=card, account=vId.account)
    
    def getDriver(self, redisConn, cardKey) -> dict[datetime.datetime, DriverValues]:
        if cardKey in self.driverCache:
            #print(f"CACHE HIT FOR: {cardKey}")
            return self.driverCache[cardKey]
        # print(f"REDIS: {cardKey}")
        tz = datetime.timezone.utc
        values = redisConn.zrangebyscore(cardKey, min=self.r_start, max=self.r_end, withscores=True)
        dictValues = {datetime.datetime.fromtimestamp(int(value[1]), tz): _split_values(value[0]) for value in values}
        self.driverCache[cardKey] = dictValues
        return dictValues


    def task(self, value):
        self.drivers = {}
        vId, vValue = value # manca il plate
        for ts, vVals in vValue.items():
            for slot, card in enumerate((vVals.driver1card, vVals.driver2card), start=1):
                if not card:
                    continue
                driverId = DriverID(card=card, account=vId.account) 
                v = self.drivers.setdefault(driverId, {})
                driver = self.getDriver(self.redisConn, _encode_driver_id(driverId))
                v[ts] = _decode_driver_values(driver[ts],vId, vVals, slot)
        return self.drivers

    def doTask(self, value):
        valueArray = value if isinstance(value, (list, np.ndarray)) else [value]
        for v in valueArray:
            result = self.task(v)
            if len(result) != 0:
                self.putResult(result)
                    

        
        
        


def register() -> UnorderedWorker:
    return WorkerTrucklinkGetVehicleDrivers


if __name__ == "__main__":
    import sys
    sys.path.append('./')
    from clients import RedisClient
    from models import RedisSettings
    worker = WorkerTrucklinkGetVehicleDrivers(**{
        "redis_connection": RedisClient(RedisSettings("localhost",6379, False, False)),
        "args": {
            "start": "-inf",
            "end": "+inf"
        }
    })
    from plugins.trucklink_parseVehicles import VehicleID, VehicleValues

    worker.doTask(
        (
        VehicleID('WMA06XZZ9KM837045','sX89HLQSrLHRzBR6Y','FX170TM'),
         OrderedDict([(datetime.datetime(2022, 10, 20, 8, 49, 8, tzinfo=datetime.timezone.utc), VehicleValues(position=Position(latitude='45.4224', longitude='9.3125', altitude='109.0000', hdop='1.2000', angle='274.3000', speed='0'), plate='FX170TM', ignition='false', tco_speed='0', rpm=None, driver1card='I-00000407308002', driver1status='4', driver2card='', driver2status='4'))])
         ))




   
    # from run_standalone import run_standalone
    # from trucklink_parseVehicles import VehicleID, VehicleValues
    # run_standalone(WorkerTrucklinkGetVehicleDrivers, iterate_list=True)