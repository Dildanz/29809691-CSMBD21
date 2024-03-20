#!/usr/bin/env python

"""
Combiner script for the MapReduce job.
Performs local aggregation of flight counts for each passenger.
Input: Key-value pairs from the mapper, where the key is the passenger ID and the value is 1
Output: Key-value pairs, where the key is the passenger ID and the value is the sum of flight counts
"""

def combiner(counts):
    # Locally aggregates the flight counts for each passenger
    # Returns the sum of the flight counts
    return sum(counts)