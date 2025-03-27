from dataclasses import dataclass
from typing import Optional

_KEY_SEP = ':'
_VEHICLES_ZSET = _KEY_SEP.join(('trucklink', 'vehicles-hs'))
_DRIVERS_ZSET = _KEY_SEP.join(('trucklink', 'drivers-hs'))

@dataclass(frozen=True)
class Position:
    latitude: str
    longitude: str
    altitude: str
    hdop: str
    angle: str
    speed: str

@dataclass(frozen=True)
class DriverID:
    card: str
    account: str

@dataclass(frozen=True)
class DriverValues:
    position: Position
    vin: str
    tacho_slot: str
    tachoTime: Optional[str] = None
    status: Optional[str] = None
    prevStatus: Optional[str] = None
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
class VehicleID:
    vin: str
    account: str
    plate: Optional[str] = None


@dataclass(frozen=True)
class VehicleValues:
    position: Position
    plate: Optional[str] = None
    tachoTime: Optional[str] = None
    outOfScope: Optional[str] = None
    ignition: Optional[str] = None
    tco_speed: Optional[str] = None
    rpm: Optional[str] = None
    driver1card: Optional[str] = None
    driver1status: Optional[str] = None
    prevDriver1status: Optional[str] = None
    driver2card: Optional[str] = None
    driver2status: Optional[str] = None


def _encode_vehicle_id(id: VehicleID) -> str:
    return _KEY_SEP.join((_VEHICLES_ZSET, id.account, id.vin))

def _encode_driver_id(id: DriverID) -> str:
    return _KEY_SEP.join((_DRIVERS_ZSET, id.account, id.card))