from datetime import datetime, time
import itertools
import logging
from typing import Any, Iterator
from collections import OrderedDict
from mpipe import UnorderedWorker
from pymongo.operations import UpdateOne

from plugins.trucklink_models import DriverID, DriverValues





class WorkerMongoUpsert(UnorderedWorker):
    def __init__(self, **kwargs):
        super().__init__()
        self.mongoConn = kwargs['mongo_connection']
        self.database: str = kwargs['args']['database']
        self.collection: str = kwargs['args']['collection']
        self.num_bulkwrite: str = kwargs['args']['num_bulkwrite']

    
    def minutes_from_midnight(self, d: datetime) -> int:
        midnight = d.combine(d.date(), time(0, 0), tzinfo=d.tzinfo)
        return int((d - midnight).total_seconds()) // 60


    def serialize_driver_page(self, instant: datetime, vals: DriverValues) -> str:       
        return ';'.join([
            str(self.minutes_from_midnight(instant)),
            vals.position.latitude,
            vals.position.longitude,
            vals.position.altitude,
            vals.position.angle,
            vals.position.speed,
            vals.position.hdop,
            vals.vin,
            vals.plate or '',
            vals.tco_speed or '',
            vals.tacho_slot,
            vals.status or '',
            vals.break_time or '',
            vals.cont_driv_time or '',
            vals.curr_acti_time or '',
            vals.tota_driv_time or '',
            vals.out_of_scope or '',
            vals.left_10hour_drives or '',
            vals.left_reduced_rests or '',
            vals.multi_manning or '',
            vals.vehicle_distance or '',
            ])

    def serialize_driver_history(self, id_: DriverID, hs: OrderedDict[datetime,DriverValues]) -> Iterator[dict[str, Any]]:
        d = vars(id_)
        field_names = [
            'minute', 'lat', 'lon', 'alt', 'angle', 'speed', 'hdop', 'vin', 'plate',
            'TCOSpeed', 'slot', 'status', 'break', 'contDriv', 'currActi',
            'totaDriv', 'outOfScope', 'left10HourDri', 'leftRedRests',
            'multiManning', 'vehicleDist']
        d['header'] = ';'.join(field_names)
        for day, page_group in itertools.groupby(hs.items(), lambda t: t[0].date()):
            d2 = d.copy()
            d2['day'] = datetime.combine(day, time())
            d2['minutes'] = [self.serialize_driver_page(*p) for p in page_group]
            yield d2

    def doTask(self, value):
        flat_drivers = {k: v for chunk in value for k, v in chunk.items()}
        
        ops: list[UpdateOne] = []
        now = datetime.now().astimezone()
        for id, history in flat_drivers.items():
            for day in self.serialize_driver_history(id, history):
                day['last_update'] = now
                filter_ = {k: day[k] for k in ('card', 'account', 'day')}
                update = {
                    '$set': {k: day[k] for k in ('header', 'last_update',)},
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
    return WorkerMongoUpsert