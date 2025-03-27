from dataclasses import dataclass
import dataclasses
from datetime import timedelta
import itertools
import json 
import history
from collections import Counter
import simplekml
import pickle



from plugins.trucklink_parseVehicles import VehicleID, VehicleValues
from plugins.trucklink_getVehicleDrivers import DriverID, DriverValues, Position

with open(f'dumps/vehicles.pickle', 'rb') as handle:
    newVehicles = pickle.load(handle)

with open(f'dumps/drivers.pickle', 'rb') as handle:
    newDrivers = pickle.load(handle)

with open(f'dumps/historydump/vehicles.pickle', 'rb') as handle:
    oldVehicles = pickle.load(handle)

with open(f'dumps/historydump/drivers.pickle', 'rb') as handle:
    oldDrivers = pickle.load(handle)



merged_dict = {}
for d in newDrivers:
    merged_dict.update(d)

newDriversCards = [key for d in newDrivers for key in d.keys()]
oldDriversCards = [x for x in oldDrivers]

missing_new = [x for x in newDriversCards if x.card not in [y.card for y in oldDriversCards]]
missing_old = [x for x in oldDriversCards if x.card not in [y.card for y in newDriversCards]]

duplicates_new = set([x for x in newDriversCards if newDriversCards.count(x) > 1])
duplicates_old = set([x for x in oldDriversCards if oldDriversCards.count(x) > 1])





problematic_vehicles = []
for v in newVehicles: #oldVehicles.items():
    vId, vSamples = v
    for sample in list(vSamples.values()):
        if (sample.driver1card in duplicates_new or sample.driver2card in duplicates_new):
            problematic_vehicles.append(v)
print("Done")



# kml = simplekml.Kml()
# for driverId, records in b.items():
#     coords = [(values.position.longitude, values.position.latitude) for date, values in records.items()]
#     kml.newlinestring(name=driverId.card, coords=coords, description=f"{driverId.card} {driverId.account}")

# kml.save(f"dumps/{filename}.kml")




# for chunk_index, chunk in enumerate(b):
#     for id, date in chunk.items():
#         for x,y in itertools.pairwise(date):
#             if (y-x > timedelta(seconds=30)):
#                 print(chunk_index, id, y-x)