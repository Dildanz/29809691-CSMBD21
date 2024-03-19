import logging
import threading
from mapper import mapper
from combiner import combiner
from reducer import reducer
from partitioner import partitioner

logging.basicConfig(filename='src/log/mapreduce.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class MapReduceJob:
    """
    MapReduce job for analysing passenger flight data files

        Class Attributes:
        config (dict) - A dictionary containing job configuration
        passenger_flight_counts(dict) - Dictionary to store intermediate data produced by mapper
        output_data (list) - List to store the final output
        airport_codes (set) - Set to store valid airport codes

        Class Methods:
        run(): Execute job
        _load_airport_codes(): Load valid airport codes
        _map(): Perform map phase
        _mapper_worker(line): Worker function for mapper threads
        _combine(): Perform combine phase
        _combiner_worker(passenger_id, counts): Worker function for combiner threads
        _reduce(): Perform reduce phase
        _reducer_worker(partition_id, partition_data): Worker function for the reducer threads
        _write_output(): Write output data
    """

    def __init__(self, config):
        """
        Initialisation method

            Parameters:
            config(dict) - A dict containing job configurations
        """
        self.config = config
        self.passenger_flight_counts= {}
        self.output_data = []
        self.airport_codes = self._load_airport_codes()

    def run(self):
        """
        This method executes the map, combine, reduce, and output phases
        """
        self._map()
        self._combine()
        self._reduce()
        self._write_output()

    def _load_airport_codes(self):
        """
        Load the valid airport codes from the airport dataset
        """
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
        """
        This method reads the passenger data, creates mapper threads to process each line,
        and collects the passenger_flight_counts produced by the mappers
        """
        with open(self.config['input_file'], 'r') as input_file:
            mapper_threads = []

            # Creates threads for each line in the passenger data
            for line in input_file:
                mapper_thread = threading.Thread(target=self._mapper_worker, args=(line,))
                mapper_thread.start()
                mapper_threads.append(mapper_thread)

            # Waits for mappers to complete
            for thread in mapper_threads:
                thread.join()

    def _mapper_worker(self, line):
        """
        This method processes a single line from the input passenger data, applies the mapper,
        and aggregates passenger_flight_counts

            Parameters:
            line(str) - A line from the input data file
        """
        key_value_pairs = mapper(line, self.airport_codes)

        # Processes each key-value pair emitted by the mapper
        for passenger_id, count in key_value_pairs:
            # Aggregates the flight counts for each passenger
            if passenger_id not in self.passenger_flight_counts:
                self.passenger_flight_counts[passenger_id] = []
            self.passenger_flight_counts[passenger_id].append(count)

    def _combine(self):
        """
        This method creates combiner threads to aggregate passenger_flight_counts
        """
        combiner_threads = []

        # Creates threads for each passenger
        for passenger_id, counts in self.passenger_flight_counts.items():
            combiner_thread = threading.Thread(target=self._combiner_worker, args=(passenger_id, counts))
            combiner_thread.start()
            combiner_threads.append(combiner_thread)

        # Waits for combiners to complete
        for thread in combiner_threads:
            thread.join()

    def _combiner_worker(self, passenger_id, counts):
        """
        This method applies the combiner to aggregate flight counts for each passenger

            Parameters:
            passenger_id(str) - The passenger ID
            counts(list) - A list of flight counts for the passenger
        """
        self.passenger_flight_counts[passenger_id] = combiner(counts)

    def _reduce(self):
        """
        This method partitions passenger_flight_counts among reducers, creates reducer threads
        to process each partition, and collects the final output data
        """
        # Partitions the intermediate data among the reducers
        partitioned_data = {}

        for passenger_id, counts in self.passenger_flight_counts.items():
            partition = partitioner(passenger_id, self.config['num_reducers'])
            if partition not in partitioned_data:
                partitioned_data[partition] = []
            partitioned_data[partition].append((passenger_id, counts))

        reducer_threads = []

        # Creates threads for each partition
        for partition, data in partitioned_data.items():
            reducer_thread = threading.Thread(target=self._reducer_worker, args=(data,))
            reducer_thread.start()
            reducer_threads.append(reducer_thread)

        # Waits for reducers to complete
        for thread in reducer_threads:
            thread.join()

    def _reducer_worker(self, partition_data):
        """
        This method applies the reducer function to compute total flight count for each passenger
        in a partition and collects the final output

            Parameters:
            partition_data (list) - A list of (passenger_id, counts) tuples for a partition.
        """        
        for passenger_id, counts in partition_data:
            total_flights = reducer(passenger_id, counts)
            self.output_data.append(f"{passenger_id}\t{total_flights}")

    def _write_output(self):
        """
        This method writes the final data to the specified output file (defined within config.yaml)
        """
        with open(self.config['output_file'], 'w') as output_file:
            output_file.write('\n'.join(self.output_data))