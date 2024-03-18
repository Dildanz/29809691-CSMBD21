def partition(data, num_partitions):
    """
    Partitioner distributes the key-value pairs among the reducers

    Parameters
    data - A dictionary of key-value pairs, where the key = passenger ID and the value = flight count
    num_partitions - The number of partitions (reducers) to distribute the data

    Returns
    dict - A dictionary of partitioned data, where the key is the partition ID and the value is a dictionary of key-value pairs belonging to that partition
    """
    partitioned_data = {}
    
    # Initialises an empty dictionary for each partition
    for i in range(num_partitions):
        partitioned_data[i] = {}
    
    # Distributes the key-value pairs among the partitions based on a hash function
    for key, value in data.items():
        partition_id = hash(key) % num_partitions
        partitioned_data[partition_id][key] = value
    
    return partitioned_data