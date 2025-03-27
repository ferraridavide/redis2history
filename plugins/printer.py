import ctypes
import multiprocessing
import time
from redis import Redis
from mpipe import UnorderedWorker


class WorkerPrinter(UnorderedWorker):
    def __init__(self, printerSharedValue: multiprocessing.Value, **kwargs):
        super().__init__()
        self.sharedValue = printerSharedValue

    @classmethod
    def assemble(cls, args, input_tube, output_tubes, size, disable_result, do_stop_task):

        # Create the workers.
        printerSharedValue = multiprocessing.Value(ctypes.c_int, 0)

        workers = []
        for ii in range(size):
            worker = cls(printerSharedValue, **args)
            worker.init2(
                input_tube,
                output_tubes,
                size,
                disable_result,
                do_stop_task,
            )
            workers.append(worker)

        # Start the workers.
        for worker in workers:
            worker.start()

    def doTask(self, value):
        with self.sharedValue.get_lock():
            self.sharedValue.value += 1
            print(f"{self.sharedValue.value} - {multiprocessing.current_process().ident} - {str(value)[0:128]}")
        # print(value)
        return value


def register() -> UnorderedWorker:
    return WorkerPrinter
