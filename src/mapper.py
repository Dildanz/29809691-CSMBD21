def mapper(line):
    try:
        passenger_id = line.strip().split(',')[0]
        return [f"{passenger_id}\t1"]
    except IndexError:
        logging.error(f"Invalid input line: {line}")
        return []