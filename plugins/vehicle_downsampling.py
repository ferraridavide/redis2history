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


class WorkerVehicleDownsampling(UnorderedWorker):
    def __init__(self, **kwargs):
        super().__init__()
        self.interval: int = int(kwargs['args']['interval'])

    def doTask(self, value):
        vehicleId = value[0]
        vehicleData = value[1]

        # ORDINO E RAGGRUPPO
        # ...ordino e raggruppo i sample in intervalli
        sorted_timestamps = sorted(vehicleData.keys())

        interval = timedelta(seconds=self.interval)  # PARAMETRO
        timesteps_groups = group_datetimes(sorted_timestamps, interval)

        downsampled_timestamps = {}

        for begin_ts, timestep in timesteps_groups.items():  # Per ogni intervallo...
            status = {}
            if len(timestep) == 0:
                continue

            "--out of scope--"
            outOfScopeSampleTimestep = [
                sample for sample in timestep if vehicleData[sample].outOfScope == 'true']
            if outOfScopeSampleTimestep:
                lastOutOfScope = outOfScopeSampleTimestep[-1]
                downsampled_timestamps[lastOutOfScope] = vehicleData[lastOutOfScope]
                continue

            # ...confronto ogni sample con quello successivo...
            for current, next in itertools.pairwise([*timestep, begin_ts + interval]):
                sample = vehicleData[current]
                status[sample.driver1status] = status.setdefault(sample.driver1status, 0) + (
                    next - current).total_seconds()  # ... per calcolare la durata totale del sample

            "--prevDriver1status--"
            if vehicleData[timestep[0]].prevDriver1status in status:
                status[vehicleData[timestep[0]
                                   ].prevDriver1status] += (timestep[0] - begin_ts).total_seconds()

            max_value = max(status.values())
            # Trovo il tipo (status) di sample che prevale in questo intervallo
            longest_status = [
                k for k, v in status.items() if v == max_value][-1]
            # In caso di pareggio, prendo l'ultimo status

            # Una volta trovato lo status prevalente...
            for t in timestep[::-1]:
                # ...prendo i dati dell'ultimo sample con quello status
                if vehicleData[t].driver1status == longest_status:
                    downsampled_timestamps[t] = vehicleData[t]
                    break
        self.putResult((vehicleId, downsampled_timestamps))
        self.putResult(None)


def register() -> UnorderedWorker:
    return WorkerVehicleDownsampling


if __name__ == "__main__":
    from run_standalone import run_standalone
    results = run_standalone(WorkerVehicleDownsampling)

    results_dict = {}
    for vehicleDict in results:
        results_dict.update({vehicleDict[0]: vehicleDict[1]})

    total_samples_after_downsampling = sum(
        len(samples) for samples in results_dict.values())
    print("Total samples after downsampling: " +
          str(total_samples_after_downsampling))
