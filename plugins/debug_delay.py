import datetime
from mpipe import UnorderedWorker
import threading
import time


class WorkerDebugDelay(UnorderedWorker):
    def __init__(self, **kwargs):
        super().__init__()
        self.delay: int = int(kwargs['args']['delay'])
        self.name: str = kwargs['args']['name']
        

    def doTask(self, value):
        print(datetime.datetime.now().strftime("%H:%M:%S") + f"\t Delay ID: {self.name} \t Delay: {self.delay} \t Value: {value}")
        time.sleep(self.delay)
        self.putResult(value)
        print(datetime.datetime.now().strftime("%H:%M:%S") + f"\t Delay ID: {self.name} \t Out: {value}")

def register() -> UnorderedWorker:
    return WorkerDebugDelay

if __name__ == "__main__":
    from run_standalone import run_standalone
    run_standalone(WorkerDebugDelay)