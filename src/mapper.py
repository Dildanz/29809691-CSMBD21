#!/usr/bin/env python

import sys
import logging

logging.basicConfig(filename='src/log/mapreduce.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def mapper():
    for line in sys.stdin:
        line = line.strip()
        try:
            passenger_id = line.split(',')[0]
            print(f"{passenger_id}\t1")
        except IndexError:
            logging.error(f"Invalid input line: {line}")

if __name__ == "__main__":
    try:
        mapper()
    except Exception as e:
        logging.error(f"Mapper error: {str(e)}")