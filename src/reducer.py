#!/usr/bin/env python

import sys
import logging
from multiprocessing import Process, Queue

logging.basicConfig(filename='src/log/mapreduce.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def reducer_worker(input_queue, output_queue):
    current_passenger = None
    flight_count = 0

    while True:
        line = input_queue.get()
        if line is None:
            break
        try:
            passenger_id, count = line.strip().split('\t', 1)
            if current_passenger == passenger_id:
                flight_count += int(count)
            else:
                if current_passenger:
                    output_queue.put(f"{current_passenger}\t{flight_count}")
                    print(f"Reducer: Processed passenger {current_passenger} with {flight_count} flights")
                current_passenger = passenger_id
                flight_count = int(count)
        except (ValueError, IndexError):
            logging.error(f"Invalid input line: {line}")

    if current_passenger:
        output_queue.put(f"{current_passenger}\t{flight_count}")
        print(f"Reducer: Processed passenger {current_passenger} with {flight_count} flights")

def reducer():
    num_workers = 4
    input_queue = Queue()
    output_queue = Queue()

    workers = []
    for i in range(num_workers):
        worker = Process(target=reducer_worker, args=(input_queue, output_queue))
        worker.start()
        workers.append(worker)
        print(f"Reducer: Started worker {i+1}")

    line_count = 0
    for line in sys.stdin:
        input_queue.put(line)
        line_count += 1
    print(f"Reducer: Read {line_count} lines from input")

    for _ in range(num_workers):
        input_queue.put(None)

    for worker in workers:
        worker.join()

    print("Reducer: All workers completed")

    while not output_queue.empty():
        print(output_queue.get())

if __name__ == "__main__":
    try:
        reducer()
    except Exception as e:
        logging.error(f"Reducer error: {str(e)}")