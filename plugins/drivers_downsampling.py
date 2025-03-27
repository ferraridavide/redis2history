from collections import OrderedDict
from datetime import timedelta
import itertools
from mpipe import UnorderedWorker


def group_datetimes(datetimes, interval):
    if (len(datetimes) == 0):
        return {}
    grouped_datetimes = {}
    current_group = []
    begin_ts = datetimes[0]

    for ts in datetimes:
        if (ts - begin_ts) < interval:
            current_group.append(ts)
        else:
            grouped_datetimes[begin_ts] = current_group
            begin_ts = begin_ts + interval
            current_group = [ts]

    if current_group:
        if begin_ts in grouped_datetimes:
            grouped_datetimes[begin_ts].extend(current_group)
        else:
            grouped_datetimes[begin_ts] = current_group

    return grouped_datetimes

def downsampling(entityData, interval, outOfScopeCriteria,currentStatus, prevStatus):

    # ORDINO E RAGGRUPPO
    sorted_timestamps = sorted(entityData.keys()) # ...ordino e raggruppo i sample in intervalli

    interval = timedelta(seconds=interval) # PARAMETRO
    timesteps_groups = group_datetimes(sorted_timestamps, interval)


    downsampled_timestamps = {}

    for begin_ts, timestep in timesteps_groups.items(): # Per ogni intervallo...
        status = OrderedDict()
        if len(timestep) == 0: continue
        
        "--out of scope--"
        outOfScopeSampleTimestep = [sample for sample in timestep if outOfScopeCriteria(entityData[sample])]
        if outOfScopeSampleTimestep:
            lastOutOfScope = outOfScopeSampleTimestep[-1]
            downsampled_timestamps[lastOutOfScope] = entityData[lastOutOfScope]
            continue

        for current, next in itertools.pairwise([*timestep, begin_ts + interval]): # ...confronto ogni sample con quello successivo...
            sample = entityData[current]
            status[currentStatus(sample)] = status.setdefault(currentStatus(sample), 0) + (next - current).total_seconds() # ... per calcolare la durata totale del sample
        
        "--prevDriver1status--"
        # Se status contiene (esiste almeno un'altro sample di questo tipo) prevStatus(entityData[timestep[0]]), aggiungo il tempo di durata di questo sample
        if prevStatus(entityData[timestep[0]]) in status:
            status[prevStatus(entityData[timestep[0]])] += (timestep[0] - begin_ts).total_seconds()
        
        max_value = max(status.values())
        longest_status = [k for k, v in status.items() if v == max_value][-1] # Trovo il tipo (status) di sample che prevale in questo intervallo
        # In caso di pareggio, prendo l'ultimo status

        for t in timestep[::-1]: # Una volta trovato lo status prevalente...
            if currentStatus(entityData[t]) == longest_status: # ...prendo i dati dell'ultimo sample con quello status 
                downsampled_timestamps[t] = entityData[t]
                break
    return downsampled_timestamps



class WorkerDriverDownsampling(UnorderedWorker):
    def __init__(self, **kwargs):
        super().__init__()
        self.interval: int = int(kwargs['args']['interval'])


    def doTask(self, value):
        driverId, driverData = list(value.items())[0]


        outOfScopeCriteria = lambda sample: sample.out_of_scope == "true"
        currentStatus = lambda sample: sample.status
        prevStatus = lambda sample: sample.prevStatus
        downsampled_timestamps = downsampling(driverData, self.interval, outOfScopeCriteria, currentStatus, prevStatus)

        self.putResult((driverId, downsampled_timestamps))
        self.putResult(None)

def register() -> UnorderedWorker:
    return WorkerDriverDownsampling

if __name__ == "__main__":
    from run_standalone import run_standalone
    results = run_standalone(WorkerDriverDownsampling)

    results_dict = {}
    for driverDict in results:
        results_dict.update({driverDict[0]: driverDict[1]})

    total_samples_after_downsampling = sum(len(samples) for samples in results_dict.values())
    print("Total samples after downsampling: " + str(total_samples_after_downsampling))