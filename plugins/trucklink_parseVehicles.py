from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from mpipe import UnorderedWorker

from plugins.trucklink_models import Position, VehicleID, VehicleValues


def _errval(d: dict[str, str], root: str) -> Optional[str]:
    return d.pop(root + '.value', None) if not d.pop(root + '.error', None) else None

def _decode_vehicle_id(key: str, values) -> VehicleID:
    toks = key.split(":")
    plate=list(values.values())[-1].plate
    return VehicleID(vin=toks[-1], account=toks[-2], plate=plate)

def _split_values(s: str) -> dict[str, str]:
    # See: https://stackoverflow.com/a/5389547
    tokens = s.split('#')
    keyvals = zip(*[iter(tokens)] * 2)
    return {kv[0]: kv[1] for kv in keyvals}

def _decode_vehicle_values(d: dict[str, str]) -> VehicleValues:
    return VehicleValues(
        position=Position(
            latitude=d.pop('position.lat', '0'),
            longitude=d.pop('position.lon', '0'),
            altitude=d.pop('position.alt', '0'),
            hdop=d.pop('position.hdop', 'NaN'),
            angle=d.pop('position.angle', '0'),
            speed=d.pop('position.speed', '0'),
        ),
        plate=_errval(d, 'licensePlate'),
        tachoTime=_errval(d, 'tachoTime'),
        outOfScope=_errval(d, 'outOfScope'),
        ignition=_errval(d, 'ignitionOn'),
        tco_speed=_errval(d, 'TCOSpeed'),
        rpm=_errval(d, 'rpm'),
        driver1card=_errval(d, 'driver1Card'),
        driver1status=_errval(d, 'driver1Status'),
        prevDriver1status=_errval(d, 'prevDriver1Status'),
        driver2card=_errval(d, 'driver2Card'),
        driver2status=_errval(d, 'driver2Status'),
)

def get_color(i: int) -> str:
    colors = ["red", "green", "yellow", "blue", "magenta", "cyan", "white"]
    return colors[i % len(colors)]


class WorkerTrucklinkParseVehicles(UnorderedWorker):

    def __init__(self, **kwargs):
        super().__init__()

    def doTask(self, input: tuple[str,dict[datetime, str]]):
        key, valueHistory = input
        vValues = {ts:_decode_vehicle_values(_split_values(value)) for ts, value in valueHistory.items()}
        self.putResult((_decode_vehicle_id(key, values=vValues), vValues))

    




def register() -> UnorderedWorker:
    return WorkerTrucklinkParseVehicles


if __name__ == "__main__":
    from run_standalone import run_standalone
    run_standalone(WorkerTrucklinkParseVehicles, iterate_list=True)