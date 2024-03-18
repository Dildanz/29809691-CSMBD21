import logging
import threading
from mapper import mapper
from combiner import combiner
from reducer import reducer
from partitioner import partition

class MapReduceJob:
    def __init__(self, config):
        self.config = config
        self.intermediate_data = {}
        self.output_data = []

    def run(self):
        self._map()
        self._combine()
        self._reduce()
        self._write_output()

    def _map(self):
        with open(self.config['input_file'], 'r') as input_file:
            mapper_threads = []
            for line in input_file:
                t = threading.Thread(target=self._mapper_worker, args=(line,))
                t.start()
                mapper_threads.append(t)
            for t in mapper_threads:
                t.join()

    def _mapper_worker(self, line):
        key_value_pairs = mapper(line)
        for pair in key_value_pairs:
            passenger_id, count = pair.split('\t')
            if passenger_id not in self.intermediate_data:
                self.intermediate_data[passenger_id] = []
            self.intermediate_data[passenger_id].append(int(count))

    def _combine(self):
        combiner_threads = []
        for passenger_id, counts in self.intermediate_data.items():
            t = threading.Thread(target=self._combiner_worker, args=(passenger_id, counts))
            t.start()
            combiner_threads.append(t)
        for t in combiner_threads:
            t.join()

    def _combiner_worker(self, passenger_id, counts):
        self.intermediate_data[passenger_id] = combiner(counts)

    def _reduce(self):
        partitioned_data = partition(self.intermediate_data, self.config['num_reducers'])
        reducer_threads = []
        for partition_id, partition_data in partitioned_data.items():
            t = threading.Thread(target=self._reducer_worker, args=(partition_id, partition_data))
            t.start()
            reducer_threads.append(t)
        for t in reducer_threads:
            t.join()

    def _reducer_worker(self, partition_id, partition_data):
        for passenger_id, counts in partition_data.items():
            if isinstance(counts, int):
                total_flights = counts
            else:
                total_flights = reducer(counts)
            self.output_data.append(f"{passenger_id}\t{total_flights}")

    def _write_output(self):
        with open(self.config['output_file'], 'w') as output_file:
            output_file.write('\n'.join(self.output_data))