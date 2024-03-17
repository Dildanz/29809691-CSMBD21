#!/usr/bin/env python

import logging
from mapper import mapper
from combiner import combiner
from reducer import reducer

logging.basicConfig(filename='src/log/mapreduce.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_mapreduce():
    try:
        with open("data/passenger_main.csv", "r") as input_file:
            logging.info("Driver: Starting mapper")
            mapper_output = mapper(input_file)
            logging.info(f"Driver: Mapper output - {mapper_output}")

            logging.info("Driver: Starting combiner")
            combiner_output = combiner(mapper_output)
            logging.info(f"Driver: Combiner output - {combiner_output}")

            logging.info("Driver: Starting reducer")
            reducer_output = reducer(combiner_output)
            logging.info(f"Driver: Reducer output - {reducer_output}")

        with open("output/passenger_flights.txt", "w") as output_file:
            output_file.write("\n".join(reducer_output))
        logging.info("Driver: Output written to passenger_flights.txt")
    except FileNotFoundError:
        logging.error("Input file not found.")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    run_mapreduce()