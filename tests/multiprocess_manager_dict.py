import multiprocessing
import random
import time




def worker(shared_dict):
    for i in range(10000):
        key = random.randint(0,9)
        new_value = random.randint(0,99)
        shared_dict[key] = new_value
        # print(f"Process {multiprocessing.current_process().name} read key {key} with value {value}")

if __name__ == '__main__':
    with multiprocessing.Manager() as manager:
        shared_dict = manager.dict({i: 0 for i in range(10)})
        processes = []

        start_time = time.time()

        for i in range(4):
            p = multiprocessing.Process(target=worker, args=(shared_dict,))
            p.start()
            processes.append(p)
        for p in processes:
            p.join()
        
        end_time = time.time()
        print(shared_dict)
        duration = end_time - start_time
        print(f"Total execution time: {duration:.2f} seconds")