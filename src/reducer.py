def reducer(counts):
    """
    Reducer computes the total flight count for each passenger

    Parameters
    counts - A list of flight counts for a passenger

    Returns
    int - The total flight count for the passenger
    """
    return sum(counts)