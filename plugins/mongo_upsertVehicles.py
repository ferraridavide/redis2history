from collections import OrderedDict
from datetime import datetime, time
import itertools
import logging
from typing import Any, Iterator
from mpipe import UnorderedWorker
from pymongo.operations import UpdateOne

from plugins.trucklink_models import VehicleID, VehicleValues



class WorkerMongoUpsertVehicles(UnorderedWorker):
    def __init__(self, **kwargs):
        super().__init__()
        self.mongoConn = kwargs['mongo_connection']
        self.database: str = kwargs['args']['database']
        self.collection: str = kwargs['args']['collection']
        self.num_bulkwrite: str = kwargs['args']['num_bulkwrite']

        
    def minutes_from_midnight(self, d: datetime) -> int:
        midnight = d.combine(d.date(), time(0, 0), tzinfo=d.tzinfo)
        return int((d - midnight).total_seconds()) // 60
    
    def serialize_vehicle_history(self, id: VehicleID, hs: OrderedDict[datetime, VehicleValues]) -> Iterator[dict[str, Any]]:
        d = vars(id)
        field_names = ['minute', 'lat', 'lon', 'alt', 'angle', 'speed', 'hdop',
                    'plate', 'ignition', 'TCOSpeed', 'rpm', 'drv1Card',
                    'drv1Status', 'drv2Card', 'drv2Status']
        d['header'] = ';'.join(field_names)
        for day, page_group in itertools.groupby(hs.items(), lambda t: t[0].date()):
            d2 = d.copy()
            d2['day'] = datetime.combine(day, time())
            d2['minutes'] = [self.serialize_vehicle_page(*p) for p in page_group]
            yield d2


    def serialize_vehicle_page(self, instant: datetime, vals: VehicleValues) -> str:
        return ';'.join(
            [str(self.minutes_from_midnight(instant)),
            vals.position.latitude,
            vals.position.longitude,
            vals.position.altitude,
            vals.position.angle,
            vals.position.speed,
            vals.position.hdop,
            vals.plate or '',
            vals.ignition or '',
            vals.tco_speed or '',
            vals.rpm or '',
            vals.driver1card or '',
            vals.driver1status or '',
            vals.driver2card or '',
            vals.driver2status or '']
        )

    def doTask(self, value):
        # flat_vehicles = {k: v for chunk in value for k, v in chunk.items()}
        ops: list[UpdateOne] = []
        now = datetime.now().astimezone()
        for id, hs in value:
            for day in self.serialize_vehicle_history(id, hs):
                day['last_update'] = now
                filter_ = {k: day[k] for k in ('vin', 'account', 'day')}
                update = {
                    '$set': {k: day[k] for k in ('header', 'plate', 'last_update')},
                    '$push': {'minutes': {'$each': day['minutes']}}}
                ops.append(UpdateOne(filter_, update, upsert=True))
                if len(ops) >= self.num_bulkwrite:
                    try:
                        self.mongoConn.bulk_write(self.database, self.collection, ops)
                    except Exception as e:
                        logging.error(e)
                    finally:
                        ops = []

        if len(ops) != 0:
            try:
                self.mongoConn.bulk_write(self.database, self.collection, ops)
            except Exception as e:
                logging.error(e)
        



     

def register() -> UnorderedWorker:
    return WorkerMongoUpsertVehicles