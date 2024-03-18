def partition(data, num_partitions):
    partitioned_data = {}
    for i in range(num_partitions):
        partitioned_data[i] = {}
    for key, value in data.items():
        partition_id = hash(key) % num_partitions
        partitioned_data[partition_id][key] = value
    return partitioned_data