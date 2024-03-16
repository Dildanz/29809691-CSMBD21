#!/usr/bin/env python

import subprocess
import logging

logging.basicConfig(filename='src/log/mapreduce.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_mapreduce():
    mapper_cmd = "python src/mapper.py"
    combiner_cmd = "python src/combiner.py"
    reducer_cmd = "python src/reducer.py"

    try:
        with open("data/passenger_main.csv", "r") as input_file:
            mapper_process = subprocess.Popen(mapper_cmd.split(), stdin=input_file, stdout=subprocess.PIPE)
            combiner_process = subprocess.Popen(combiner_cmd.split(), stdin=mapper_process.stdout, stdout=subprocess.PIPE)
            reducer_process = subprocess.Popen(reducer_cmd.split(), stdin=combiner_process.stdout, stdout=subprocess.PIPE)
            mapper_process.stdout.close()
            combiner_process.stdout.close()
            output, _ = reducer_process.communicate()

        with open("output/passenger_flights.txt", "w") as output_file:
            output_file.write(output.decode())
    except FileNotFoundError:
        logging.error("Input file not found.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running MapReduce: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    run_mapreduce()