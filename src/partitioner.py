#!/usr/bin/env python

"""
Partitioner script for the MapReduce job.
Assigns a partition to a passenger ID based on a hash function.
Input: Passenger ID and the number of partitions
Output: The assigned partition number
"""

def partitioner(passenger_id, num_partitions):
    # Assigns a partition to a passenger ID
    # Returns the assigned partition number
    return hash(passenger_id) % num_partitions