from mpipe import UnorderedWorker


class WorkerDebugGen(UnorderedWorker):
    def __init__(self, **kwargs):
        super().__init__()
        

    def doTask(self, value):
        for key in range(1,200000):
            self.putResult(key)
        self.putResult(None)

def register() -> UnorderedWorker:
    return WorkerDebugGen

if __name__ == "__main__":
    from run_standalone import run_standalone
    run_standalone(WorkerDebugGen)