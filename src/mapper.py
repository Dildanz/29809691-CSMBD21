#!/usr/bin/env python

import logging
import re

logging.basicConfig(filename='src/log/mapreduce.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def is_valid_passenger_record(record):
    if len(record) != 6:
        return False

    passenger_id, flight_id, from_airport, to_airport, departure_time, flight_time = record

    if not passenger_id or not flight_id or not from_airport or not to_airport:
        return False

    if not re.match(r'^\d+$', departure_time):
        return False

    if not re.match(r'^\d+$', flight_time):
        return False

    return True

def mapper(line, airport_codes):
    record = line.strip().split(',')

    if not is_valid_passenger_record(record):
        logging.warning(f"Invalid passenger record: {line.strip()}")
        return []

    passenger_id, flight_id, from_airport, to_airport, departure_time, flight_time = record

    if from_airport not in airport_codes or to_airport not in airport_codes:
        logging.warning(f"Invalid airport code: {from_airport} or {to_airport}")
        return []

    return [(passenger_id, 1)]