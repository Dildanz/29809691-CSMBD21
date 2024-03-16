#!/usr/bin/env python

import sys
import logging

logging.basicConfig(filename='src/log/mapreduce.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def reducer():
    current_passenger = None
    flight_count = 0

    for line in sys.stdin:
        line = line.strip()
        try:
            passenger_id, count = line.split('\t', 1)
            if current_passenger == passenger_id:
                flight_count += int(count)
            else:
                if current_passenger:
                    print(f"{current_passenger}\t{flight_count}")
                current_passenger = passenger_id
                flight_count = int(count)
        except (ValueError, IndexError):
            logging.error(f"Invalid input line: {line}")

    if current_passenger:
        print(f"{current_passenger}\t{flight_count}")

if __name__ == "__main__":
    try:
        reducer()
    except Exception as e:
        logging.error(f"Reducer error: {str(e)}")