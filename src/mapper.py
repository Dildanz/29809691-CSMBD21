#!/usr/bin/env python

import sys
import logging
from multiprocessing import Process, Queue

logging.basicConfig(filename='src/log/mapreduce.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def mapper_worker(input_queue, output_queue):
    while True:
        line = input_queue.get()
        if line is None:
            break
        try:
            passenger_id = line.strip().split(',')[0]
            output_queue.put(f"{passenger_id}\t1")
            logging.info(f"Mapper: Processed passenger {passenger_id}")
        except IndexError:
            logging.error(f"Invalid input line: {line}")

def mapper():
    num_workers = 4
    input_queue = Queue()
    output_queue = Queue()

    workers = []
    for i in range(num_workers):
        worker = Process(target=mapper_worker, args=(input_queue, output_queue))
        worker.start()
        workers.append(worker)
        logging.info(f"Mapper: Started worker {i+1}")

    line_count = 0
    for line in sys.stdin:
        input_queue.put(line)
        line_count += 1
        if line_count % 1000 == 0:
            logging.info(f"Mapper: Processed {line_count} lines")
    logging.info(f"Mapper: Read {line_count} lines from input")

    for _ in range(num_workers):
        input_queue.put(None)

    for worker in workers:
        worker.join()

    logging.info("Mapper: All workers completed")

    output_count = 0
    while not output_queue.empty():
        print(output_queue.get())
        output_count += 1
    logging.info(f"Mapper: Produced {output_count} output pairs")

if __name__ == "__main__":
    try:
        mapper()
    except Exception as e:
        logging.error(f"Mapper error: {str(e)}")