#!/usr/bin/env python

import logging
import threading
from mapper import mapper
from combiner import combiner
from reducer import reducer
from partitioner import partitioner

logging.basicConfig(filename='src/log/mapreduce.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MapReduceJob:

    def __init__(self, config):
        self.config = config
        self.passenger_flight_counts = {}
        self.output_data = []
        self.airport_codes = self._load_airport_codes()

    def run(self):
        self._map()
        self._combine()
        self._reduce()
        self._write_output()

    def _load_airport_codes(self):
        airport_codes = set()
        with open(self.config['airport_file'], 'r') as file:
            for line in file:
                try:
                    airport_code = line.strip().split(',')[1]
                    airport_codes.add(airport_code)
                except IndexError:
                    logging.warning(f"Invalid airport data format: {line.strip()}")
        return airport_codes

    def _map(self):
        with open(self.config['input_file'], 'r') as input_file:
            mapper_threads = []

            for line in input_file:
                mapper_thread = threading.Thread(target=self._mapper_worker, args=(line,))
                mapper_thread.start()
                mapper_threads.append(mapper_thread)

            for thread in mapper_threads:
                thread.join()

    def _mapper_worker(self, line):
        key_value_pairs = mapper(line, self.airport_codes)

        for passenger_id, count in key_value_pairs:
            if passenger_id not in self.passenger_flight_counts:
                self.passenger_flight_counts[passenger_id] = []
            self.passenger_flight_counts[passenger_id].append(count)

    def _combine(self):
        combiner_threads = []

        for passenger_id, counts in self.passenger_flight_counts.items():
            combiner_thread = threading.Thread(target=self._combiner_worker, args=(passenger_id, counts))
            combiner_thread.start()
            combiner_threads.append(combiner_thread)

        for thread in combiner_threads:
            thread.join()

    def _combiner_worker(self, passenger_id, counts):
        self.passenger_flight_counts[passenger_id] = combiner(counts)

    def _reduce(self):
        partitioned_data = {}

        for passenger_id, counts in self.passenger_flight_counts.items():
            partition = partitioner(passenger_id, self.config['num_reducers'])
            if partition not in partitioned_data:
                partitioned_data[partition] = []
            partitioned_data[partition].append((passenger_id, counts))

        reducer_threads = []

        for partition, data in partitioned_data.items():
            reducer_thread = threading.Thread(target=self._reducer_worker, args=(data,))
            reducer_thread.start()
            reducer_threads.append(reducer_thread)

        for thread in reducer_threads:
            thread.join()

    def _reducer_worker(self, partition_data):
        for passenger_id, counts in partition_data:
            total_flights = reducer(passenger_id, counts)
            self.output_data.append(f"{passenger_id}\t{total_flights}")

    def _write_output(self):
        with open(self.config['output_file'], 'w') as output_file:
            output_file.write('\n'.join(self.output_data))