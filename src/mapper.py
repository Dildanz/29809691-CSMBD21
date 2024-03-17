#!/usr/bin/env python

import logging

logging.basicConfig(filename='src/log/mapreduce.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def mapper(input_file):
    output = []
    for line in input_file:
        try:
            passenger_id = line.strip().split(',')[0]
            output.append(f"{passenger_id}\t1")
            logging.info(f"Mapper: Processed passenger {passenger_id}")
        except IndexError:
            logging.error(f"Invalid input line: {line}")
    return output