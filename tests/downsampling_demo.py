from datetime import timedelta
import itertools
import pickle


from plugins.trucklink_parseVehicles import VehicleID, VehicleValues
from plugins.trucklink_getVehicleDrivers import DriverID, DriverValues, Position

with open(f'../dumps/vehicles.pickle', 'rb') as handle:
    newVehicles = pickle.load(handle)

# with open(f'WorkerVehicleDownsampling_20231106_160752.pickle', 'rb') as handle:
#     dumped = pickle.load(handle)

result_dict = {}
for vehicleDict in newVehicles:
    result_dict.update({vehicleDict[0]: vehicleDict[1]})

# result_dict_after = {}
# for vehicleDict in dumped:
#     result_dict_after.update({vehicleDict[0]: vehicleDict[1]})

total_samples_before_downsampling = sum(len(samples) for samples in result_dict.values())
# total_samples_after_downsampling = sum(len(samples) for samples in result_dict_after.values())

print("Total samples before downsampling: " + str(total_samples_before_downsampling))
# print("Total samples after downsampling: " + str(total_samples_after_downsampling))


def group_datetimes(datetimes, interval):
    grouped_datetimes = []
    current_group = []
    begin_ts = datetimes[0]

    for ts in datetimes:
        if (ts - begin_ts) < interval:
            current_group.append(ts)
        else:
            grouped_datetimes.append(current_group)
            begin_ts = ts
            current_group = [ts]

    if current_group:
        grouped_datetimes.append(current_group)

    return grouped_datetimes



print("Vehicle count: " + str(len(result_dict)))

vehicleVins = [x.vin for x in result_dict.keys()]
dupVehicleVins = list(set([x for x in vehicleVins if vehicleVins.count(x) > 1]))
print("Duplicate vehicle vins: " + str(len(dupVehicleVins)))

import simplekml
kml = simplekml.Kml()
colors = {'1': 'FFFF0000', '2':'FF00FF00', '3':'FF0000FF', '4':'FFFFFF00', '5':'FF00FFFF'}


downsampled_vehicles = {}
for vehicleId, vehicleData in result_dict.items(): # Per ogni veicolo...
    sorted_timestamps = sorted(vehicleData.keys()) # ...ordino e raggruppo i sample in intervalli
    "--debug only--"
    if (len(sorted_timestamps) != 0):
        folder = kml.newfolder(name=f"{vehicleId.vin} - {vehicleId.account}")
        for index, timestamp_pair in enumerate(itertools.pairwise(sorted_timestamps)):
                coords = [(vehicleData[x].position.longitude, vehicleData[x].position.latitude) for x in timestamp_pair]
                line = folder.newlinestring(name=str(index), coords=coords)
                line.style.linestyle.color = colors[vehicleData[timestamp_pair[0]].driver1status] if vehicleData[timestamp_pair[0]].driver1status in colors else '000000'
        folder.description = '\n'.join([f"{i}: status {vehicleData[dt].driver1status} at {dt}" for i, dt in enumerate(sorted_timestamps)])
    ""
    interval = timedelta(seconds=60)
    timesteps = group_datetimes(sorted_timestamps, interval)

    downsampled_timestamps = {}
    for timestep in timesteps: # Per ogni intervallo...
        status = {}
        if len(timestep) == 0: continue
        
        "--out of scope--"
        outOfScopeSampleTimestep = [sample for sample in timestep if vehicleData[sample].outOfScope == 'true']
        if outOfScopeSampleTimestep:
            lastOutOfScope = outOfScopeSampleTimestep[-1]
            downsampled_timestamps[lastOutOfScope] = vehicleData[lastOutOfScope]
            continue

        "--prevDriver1status--"
        firstSample = vehicleData[timestep[0]]
        if firstSample.prevDriver1status == firstSample.driver1status:
            status[firstSample.driver1status] = status.setdefault(firstSample.driver1status, 0) + timestep[0].second

        for current, next in itertools.pairwise([*timestep, timestep[0] + interval]): # ...confronto ogni sample con quello successivo...
            sample = vehicleData[current]
            status[sample.driver1status] = status.setdefault(sample.driver1status, 0) + (next - current).total_seconds() # ... per calcolare la durata totale del sample
        longest_status = max(status, key=status.get) # Trovo il tipo (status) di sample che prevale in questo intervallo

        for t in timestep[::-1]: # Una volta trovato lo status prevalente...
            if vehicleData[t].driver1status == longest_status: # ...prendo i dati dell'ultimo sample con quello status 
                downsampled_timestamps[t] = vehicleData[t]
                break
        
        
    downsampled_vehicles[vehicleId] = downsampled_timestamps
    "--debug only--"
    if (len(downsampled_timestamps) != 0):
        folder_downsampled = kml.newfolder(name=f"{vehicleId.vin} - {vehicleId.account} DOWNSAMPLED")
        for index, (timestamp, values) in enumerate(downsampled_timestamps.items()):
            pnt = folder_downsampled.newpoint(name=str(index), coords=[(values.position.longitude, values.position.latitude)])
            pnt.style.iconstyle.color = colors[values.driver1status] if values.driver1status in colors else '000000'
            pnt.style.iconstyle.scale = 2
            pnt.style.iconstyle.icon.href = "http://maps.google.com/mapfiles/kml/shapes/placemark_circle.png"
        folder_downsampled.description = '\n'.join([f"{i}: status {vehicleData[dt].driver1status} at {dt}" for i, dt in enumerate(downsampled_timestamps)])
        
    ""
total_samples_before_downsampling = sum(len(samples) for samples in result_dict.values())
total_samples_after_downsampling = sum(len(samples) for samples in downsampled_vehicles.values())

print("Total samples before downsampling: " + str(total_samples_before_downsampling))
print("Total samples after downsampling: " + str(total_samples_after_downsampling))




kml.save(f"dumps/test.kml")


    