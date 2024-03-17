#!/usr/bin/env python

def combiner(input_data):
    output = []
    current_passenger = None
    flight_count = 0

    for line in input_data:
        line = line.strip()
        try:
            passenger_id, count = line.split('\t', 1)
            if current_passenger == passenger_id:
                flight_count += int(count)
            else:
                if current_passenger:
                    output.append(f"{current_passenger}\t{flight_count}")
                current_passenger = passenger_id
                flight_count = int(count)
        except (ValueError, IndexError):
            continue

    if current_passenger:
        output.append(f"{current_passenger}\t{flight_count}")

    return output