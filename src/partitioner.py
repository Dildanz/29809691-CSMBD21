#!/usr/bin/env python

def partitioner(passenger_id, num_partitions):
    return hash(passenger_id) % num_partitions