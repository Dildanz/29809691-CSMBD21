#!/usr/bin/env python

import logging

logging.basicConfig(filename='src/log/mapreduce.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def reducer(input_data):
    output = []
    current_passenger = None
    flight_count = 0

    for line in input_data:
        line = line.strip()
        try:
            passenger_id, count = line.split('\t', 1)
            if current_passenger == passenger_id:
                flight_count += int(count)
            else:
                if current_passenger:
                    output.append(f"{current_passenger}\t{flight_count}")
                    logging.info(f"Reducer: Processed passenger {current_passenger} with {flight_count} flights")
                current_passenger = passenger_id
                flight_count = int(count)
        except (ValueError, IndexError):
            logging.error(f"Invalid input line: {line}")

    if current_passenger:
        output.append(f"{current_passenger}\t{flight_count}")
        logging.info(f"Reducer: Processed passenger {current_passenger} with {flight_count} flights")

    return output