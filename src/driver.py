#!/usr/bin/env python

"""
Driver script for the MapReduce job.
Loads the job configuration and runs the MapReduce job.
Input: Configuration file (config.yaml)
Output: None (the output is written to the specified output file by the MapReduce job)
"""

import yaml
from main_job import MapReduceJob

def main():
    # Main function that runs the MapReduce job
    with open('config.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)

    job = MapReduceJob(config)
    job.run()

if __name__ == "__main__":
    main()