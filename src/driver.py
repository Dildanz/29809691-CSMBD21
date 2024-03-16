#!/usr/bin/env python

import subprocess

def run_mapreduce():
    mapper_cmd = "python src/mapper.py"
    reducer_cmd = "python src/reducer.py"

    with open("data/passenger_main.csv", "r") as input_file:
        mapper_process = subprocess.Popen(mapper_cmd.split(), stdin=input_file, stdout=subprocess.PIPE)
        reducer_process = subprocess.Popen(reducer_cmd.split(), stdin=mapper_process.stdout, stdout=subprocess.PIPE)
        mapper_process.stdout.close()
        output, _ = reducer_process.communicate()

    with open("output/passenger_flights.txt", "w") as output_file:
        output_file.write(output.decode())

if __name__ == "__main__":
    run_mapreduce()