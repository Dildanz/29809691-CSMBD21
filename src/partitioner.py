def partitioner(passenger_id, num_partitions):
    """
    Partitioner distributes the key-value pairs among the reducers

    Parameters
    passenger_id - The passenger ID
    num_partitions - The number of partitions

    Returns
    int - Assigned partition number
    """
    return hash(passenger_id) % num_partitions