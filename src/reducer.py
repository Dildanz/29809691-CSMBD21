#!/usr/bin/env python

import logging

logging.basicConfig(filename='src/log/mapreduce.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def reducer(passenger_id, counts):
    """
    Reducer computes the total flight count for each passenger

    Parameters
    passenger_id - The passenger ID
    counts - A list of flight counts for a passenger

    Returns
    tuple - Contains the passenger ID and the total flight count
    """
    # Check if counts is an integer
    if isinstance(counts, int):
        total_flights = counts
    else:
        total_flights = sum(counts)
    
    logging.info(f"Reducer: Processed passenger {passenger_id} with {total_flights} flights")
    return (passenger_id, total_flights)