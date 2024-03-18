class MapReduceJob:
    """
    MapReduce job for analysing passenger flight data files

        Class Attributes:
        config (dict) - A dictionary containing job configuration
        passenger_flight_counts(dict) - Dictionary to store intermediate data produced by mapper
        output_data (list) - List to store the final output

        Class Methods:
        run(): Execute job
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

    def run(self):
        """
        This method executes the map, combine, reduce, and output phases
        """
        self._map()
        self._combine()
        self._reduce()
        self._write_output()

    def _map(self):
        """
        This method reads the passenger data, creates mapper threads to process each line,
        and collects the passenger_flight_counts produced by the mappers
        """
        with open(self.config['input_file'], 'r') as input_file:
            mapper_threads = []

            # Creates threads for each line in the passenger data
            for line in input_file:
                t = threading.Thread(target=self._mapper_worker, args=(line,))
                t.start()
                mapper_threads.append(t)

            # Waits for mappers to complete
            for t in mapper_threads:
                t.join()

    def _mapper_worker(self, line):
        """
        This method processes a single line from the input passenger data, applies the mapper,
        and aggregates passenger_flight_counts

            Parameters:
            line(str) - A line from the input data file
        """
        key_value_pairs = mapper(line)

        # Processes each key-value pair emitted by the mapper
        for pair in key_value_pairs:
            passenger_id, count = pair.split('\t')

            # Aggregates the flight counts for each passenger
            if passenger_id not in self.passenger_flight_counts:
                self.passenger_flight_counts[passenger_id] = []
            self.passenger_flight_counts[passenger_id].append(int(count))

    def _combine(self):
        """
        This method creates combiner threads to aggregate passenger_flight_counts
        """
        combiner_threads = []

        # Creates threads for each passenger
        for passenger_id, counts in self.passenger_flight_counts.items():
            t = threading.Thread(target=self._combiner_worker, args=(passenger_id, counts))
            t.start()
            combiner_threads.append(t)

        # Waits for combiners to complete
        for t in combiner_threads:
            t.join()

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
        partitioned_data = partition(self.passenger_flight_counts, self.config['num_reducers'])

        reducer_threads = []

        # Creates threads for each partition
        for partition_id, partition_data in partitioned_data.items():
            t = threading.Thread(target=self._reducer_worker, args=(partition_id, partition_data))
            t.start()
            reducer_threads.append(t)

        # Waits for reducers to complete
        for t in reducer_threads:
            t.join()

    def _reducer_worker(self, partition_id, partition_data):
        """
        This method processes a single partition of passenger_flight_counts, applies reducer function,
        then collects the final output data

        Parameters:
            partition_id:
                int: The partition ID
            partition_data:
                dict: A dictionary of key-value pairs belonging to the partition
        """
        for passenger_id, counts in partition_data.items():
            # Checks if counts is already a single value or a list
            if isinstance(counts, int):
                total_flights = counts
            else:
                # Computes total flight counts
                total_flights = reducer(counts)

            # Appends passenger IDs + total flight count to output data
            self.output_data.append(f"{passenger_id}\t{total_flights}")

    def _write_output(self):
        """
        This method writes the final data to the specified output file (defined within config.yaml)
        """
        with open(self.config['output_file'], 'w') as output_file:
            output_file.write('\n'.join(self.output_data))