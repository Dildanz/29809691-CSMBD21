#!/usr/bin/env python

"""
Reducer script for the MapReduce job.
Computes the total flight count for each passenger.
Input: Key-value pairs from the combiner, where the key is the passenger ID and the value is the sum of flight counts
Output: Key-value pairs, where the key is the passenger ID and the value is the total flight count
"""

import logging

logging.basicConfig(filename='src/log/mapreduce.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def reducer(passenger_id, counts):
    # Computes the total flight count for each passenger
    # Returns a tuple containing the passenger ID and the total flight count
    if isinstance(counts, int):
        total_flights = counts
    else:
        total_flights = sum(counts)
    
    logging.info(f"Reducer: Processed passenger {passenger_id} with {total_flights} flights")
    
    return (passenger_id, total_flights)