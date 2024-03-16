#!/usr/bin/env python

import sys

def reducer():
    current_passenger = None
    flight_count = 0

    for line in sys.stdin:
        line = line.strip()
        passenger_id, count = line.split('\t', 1)

        if current_passenger == passenger_id:
            flight_count += int(count)
        else:
            if current_passenger:
                print(f"{current_passenger}\t{flight_count}")
            current_passenger = passenger_id
            flight_count = int(count)

    if current_passenger:
        print(f"{current_passenger}\t{flight_count}")

if __name__ == "__main__":
    reducer()