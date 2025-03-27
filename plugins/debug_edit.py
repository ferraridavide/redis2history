
from mpipe import UnorderedWorker

class WorkerDebugEdit(UnorderedWorker):
    def __init__(self, **kwargs):
        super().__init__()
        

    def doTask(self, value):
        self.putResult(value + 1)

def register() -> UnorderedWorker:
    return WorkerDebugEdit

if __name__ == "__main__":
    from run_standalone import run_standalone
    run_standalone(WorkerDebugEdit)