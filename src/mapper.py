import logging

logging.basicConfig(filename='src/log/mapreduce.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def mapper(line, airport_codes):
    """
    Mapper processes each line of the passenger data

    Parameters
    line - A line from the input data file

    Returns
    list - A list of key-value pairs, where the key is the passenger ID and the value is 1
    """
    try:
        # Extracts the passenger ID from the line
        passenger_id, flight_id, from_airport, to_airport, departure_time, flight_time = line.strip().split(',')
        
        # Validate airport codes against the airport dataset
        if from_airport in airport_codes and to_airport in airport_codes:
            return [(passenger_id, 1)]
        else:
            logging.error(f"Invalid airport code: {from_airport} or {to_airport}")
            return []
    except ValueError:
        logging.error(f"Invalid input line: {line}")
        return []