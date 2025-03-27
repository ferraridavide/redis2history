from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional, TypeVar


@dataclass(frozen=True)
class Position:
    latitude: str
    longitude: str
    altitude: str
    hdop: str
    angle: str
    speed: str




@dataclass(frozen=True)
class DriverValues:
    position: Position
    vin: str
    tacho_slot: str
    status: Optional[str] = None
    plate: Optional[str] = None
    tco_speed: Optional[str] = None
    break_time: Optional[str] = None
    cont_driv_time: Optional[str] = None
    curr_acti_time: Optional[str] = None
    tota_driv_time: Optional[str] = None
    out_of_scope: Optional[str] = None
    multi_manning: Optional[str] = None
    left_10hour_drives: Optional[str] = None
    left_reduced_rests: Optional[str] = None
    vehicle_distance: Optional[str] = None

@dataclass(frozen=True)
class DriverID:
    card: str
    account: str

@dataclass(frozen=True)
class VehicleID:
    vin: str
    account: str
    plate: Optional[str] = None


@dataclass(frozen=True)
class VehicleValues:
    position: Position
    plate: Optional[str] = None
    ignition: Optional[str] = None
    tco_speed: Optional[str] = None
    rpm: Optional[str] = None
    driver1card: Optional[str] = None
    driver1status: Optional[str] = None
    driver2card: Optional[str] = None
    driver2status: Optional[str] = None


@dataclass(frozen=True)
class TrailerID:
    vin: str
    account: str
    plate: Optional[str] = None


@dataclass(frozen=True)
class TrailerValues:
    position: Position
    plate: Optional[str] = None


ID = TypeVar('ID', DriverID, VehicleID, TrailerID)
VAL = TypeVar('VAL', DriverValues, VehicleValues, TrailerValues, dict[str, str])


class History(OrderedDict[datetime, VAL]):
    def downsample(self, freq: timedelta) -> History[VAL]:
        def filter(hs: History) -> Iterator[tuple[datetime, VAL]]:
            old_tm = datetime.min.replace(tzinfo=timezone.utc)
            for tm, val in hs.items():
                if tm >= old_tm + freq:
                    yield tm, val
                    old_tm = tm
        return History(filter(self))

    def at(self, i: int) -> tuple[datetime, VAL]:
        if i < 0:
            it = iter(reversed(self.items()))
            i = (i * (-1)) - 1
        else:
            it = iter(self.items())
        for _ in range(i):
            next(it)
        return next(it)


class HistoryColl(dict[ID, History[VAL]]):
    pass
