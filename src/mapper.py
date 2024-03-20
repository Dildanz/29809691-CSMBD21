#!/usr/bin/env python

"""
Mapper script for the MapReduce job.
Reads input data, validates passenger records and airport codes, and emits key-value pairs.
Input: Lines from the passenger data file
Output: Key-value pairs, where the key is the passenger ID and the value is 1
"""

import logging
import re

logging.basicConfig(filename='src/log/mapreduce.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def is_valid_passenger_record(record):
    # Validates each passenger record
    # Returns True if the record is valid or False if not valid
    if len(record) != 6:
        return False

    passenger_id, flight_id, from_airport, to_airport, departure_time, flight_time = record

    if not passenger_id or not flight_id or not from_airport or not to_airport:
        return False

    if not re.match(r'^[A-Z]{3}\d{4}[A-Z]{2}\d$', passenger_id):
        return False

    if not re.match(r'^\d+$', departure_time):
        return False

    if not re.match(r'^\d+$', flight_time):
        return False

    return True

def mapper(line, airport_codes):
    # Maps each input line to key-value pairs
    # Returns a list of key-value pairs, where the key is the passenger ID and the value is 1
    record = line.strip().split(',')

    if not is_valid_passenger_record(record):
        logging.warning(f"Invalid passenger record: {line.strip()}")
        return []

    passenger_id, flight_id, from_airport, to_airport, departure_time, flight_time = record

    if from_airport not in airport_codes or to_airport not in airport_codes:
        logging.warning(f"Invalid airport code: {from_airport} or {to_airport}")
        return []

    return [(passenger_id, 1)]