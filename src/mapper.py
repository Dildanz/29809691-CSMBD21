#!/usr/bin/env python

import sys

def mapper():
    for line in sys.stdin:
        line = line.strip()
        passenger_id = line.split(',')[0]
        print(f"{passenger_id}\t1")

if __name__ == "__main__":
    mapper()