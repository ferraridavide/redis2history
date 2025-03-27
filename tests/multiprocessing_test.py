import multiprocessing

N_KEYS = 10

def worker(num, d):
    key = 'key_{}'.format(num)
    if key in d:
        value = d[key]
    if num % 20 == 0:
        d[key] = "SET"
        

def sequential():
    d = {}
    precondition_dict(d)
    for i in range(N_KEYS):
        worker(i, d)
    print(d)

def parallel():
    with multiprocessing.Manager() as manager:
        d = manager.dict()
        precondition_dict(d)
        processes = [multiprocessing.Process(target=worker, args=(i, d)) for i in range(N_KEYS)]
        for process in processes:
            process.start()
        for process in processes:
            process.join()
        print(d)


def precondition_dict(d):
    for i in range(int(N_KEYS / 4)):
        key = 'key_{}'.format(i * 4)
        d[key] = []


if __name__ == '__main__':
    import time
    start = time.time()
    parallel()
    end = time.time()
    print(end-start)











































































































# from multiprocessing.managers import BaseManager
# from multiprocessing import Process, current_process
# import multiprocessing
# import time
# from redis import Redis


# class UnpickableClass:
#     def __getstate__(self):
#         raise TypeError("This class is not pickable")
    
#     def __init__(self):
#         self.value = 0
#         self.connection = Redis(host='localhost', port=6379, ssl=False, decode_responses=True)

#     def scan(self, key_pattern):
#         keys = list(self.connection.scan_iter(key_pattern))
#         print(f"Process {current_process().name} - UnpickableClass.scan")
#         return len(keys)

#     def increment(self):
#         self.value += 1

#     def get_value(self):
#         return self.value


# class UnpickableClassProxy:
#     def __init__(self):
#         self.data = UnpickableClass()
    
#     def scan(self, key_pattern):
#         return self.data.scan(key_pattern)

#     def increment(self):
#         return self.data.increment()

#     def get_value(self):
#         return self.data.get_value()

# BaseManager.register('UnpickableClassProxy', UnpickableClassProxy)



# def worker(proxy):
#     value = proxy.scan("trucklink:vehicles-hs:*")
#     print(f"Process {current_process().name} - Value after increment: {value}")

# if __name__ == "__main__":
#     manager = BaseManager()
#     manager.start()


#     # Create and share the proxy object
#     shared_proxy = manager.UnpickableClassProxy()

#     num_processes = 1
#     processes = []

#     # Spawn worker processes
#     for n in range(num_processes):
#         p = Process(target=worker, args=(shared_proxy,))
#         processes.append(p)
#         p.start()

#     # Join the worker processes
#     for p in processes:
#         p.join()

#     print("Final value: {}".format(shared_proxy.get_value()))