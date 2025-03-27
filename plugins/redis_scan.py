from mpipe import UnorderedWorker


class WorkerRedisScan(UnorderedWorker):
    def __init__(self, **kwargs):
        super().__init__()
        self.redisConn = kwargs['redis_connection']
        self.key_pattern: str = kwargs['args']['key_pattern']
        

    def doTask(self, value):
        keys = self.redisConn.scan_iter(self.key_pattern)
        for key in keys:
            self.putResult(key)
        self.putResult(None)

def register() -> UnorderedWorker:
    return WorkerRedisScan

if __name__ == "__main__":
    from run_standalone import run_standalone
    run_standalone(WorkerRedisScan)