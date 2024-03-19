#!/usr/bin/env python

import logging

logging.basicConfig(filename='src/log/mapreduce.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def reducer(passenger_id, counts):
    if isinstance(counts, int):
        total_flights = counts
    else:
        total_flights = sum(counts)
    
    logging.info(f"Reducer: Processed passenger {passenger_id} with {total_flights} flights")
    
    return (passenger_id, total_flights)