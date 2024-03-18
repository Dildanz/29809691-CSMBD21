def mapper(line):
    """
    Mapper processes each line of the passenger data

    Parameters
    line - A line from the input data file

    Returns
    list - A list of key-value pairs, where the key is the passenger ID and the value is 1
    """
    try:
        # Extracts the passenger ID from the line
        passenger_id = line.strip().split(',')[0]
        
        # Returns a key-value pair with the passenger ID as the key and 1 as the value
        return [f"{passenger_id}\t1"]
    except IndexError:
        # Logs an error if the line does not have the expected format
        logging.error(f"Invalid input line: {line}")
        return []