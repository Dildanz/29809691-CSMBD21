#!/usr/bin/env python

import subprocess
import logging
from multiprocessing import Pool

logging.basicConfig(filename='src/log/mapreduce.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_combiner(input_data):
    combiner_cmd = "python src/combiner.py"
    combiner_process = subprocess.Popen(combiner_cmd.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output, error = combiner_process.communicate(input=input_data)
    if combiner_process.returncode != 0:
        raise subprocess.CalledProcessError(combiner_process.returncode, combiner_cmd, output=output, stderr=error)
    return output

def run_reducer(input_data):
    reducer_cmd = "python src/reducer.py"
    reducer_process = subprocess.Popen(reducer_cmd.split(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output, error = reducer_process.communicate(input=input_data)
    if reducer_process.returncode != 0:
        raise subprocess.CalledProcessError(reducer_process.returncode, reducer_cmd, output=output, stderr=error)
    return output

def run_mapreduce():
    mapper_cmd = "python src/mapper.py"

    try:
        with open("data/passenger_main.csv", "r") as input_file:
            print("Driver: Starting mapper process")
            mapper_process = subprocess.Popen(mapper_cmd.split(), stdin=input_file, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            mapper_output, mapper_error = mapper_process.communicate()
            print("Driver: Mapper process completed")
            if mapper_process.returncode != 0:
                raise subprocess.CalledProcessError(mapper_process.returncode, mapper_cmd, output=mapper_output, stderr=mapper_error)

        print("Driver: Starting combiner and reducer processes")
        with Pool() as pool:
            combiner_output = pool.map(run_combiner, [mapper_output])
            reducer_output = pool.map(run_reducer, combiner_output)
        print("Driver: Combiner and reducer processes completed")

        with open("output/passenger_flights.txt", "w") as output_file:
            output_file.write("\n".join(reducer_output))
        print("Driver: Output written to passenger_flights.txt")
    except FileNotFoundError:
        logging.error("Input file not found.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running MapReduce: {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    run_mapreduce()