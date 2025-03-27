import multiprocessing
import random
import time

class SharedDict():
    def __init__(self, dictionary):
        self.dictionary = dictionary
        self.lock = multiprocessing.Lock()
        
    def read(self, key, worker):
        with self.lock:
            value = self.dictionary[key]
        return value
        
    def write(self, key, value):
        with self.lock:
            self.dictionary[key] = value

def worker(shared_dict):
    for i in range(10000):
        key = random.randint(0,9)
        shared_dict.write(key, shared_dict.read(key, worker) + 1)
        # print(f"Process {multiprocessing.current_process().name} read key {key} with value {value}")

if __name__ == '__main__':
    dictionary = {i: 0 for i in range(10)}
    shared_dict = SharedDict(dictionary)
    processes = []

    start_time = time.time()

    for i in range(4):
        p = multiprocessing.Process(target=worker, args=(shared_dict,))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()


    end_time = time.time()
    print(shared_dict.dictionary)
    duration = end_time - start_time
    print(f"Total execution time: {duration:.2f} seconds")
