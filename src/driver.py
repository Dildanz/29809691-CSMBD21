import yaml
from main_job import MapReduceJob

def main():
    """
    Runs the MapReduce job.
    """
    # Loads configuration YAML
    with open('config.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)
    
    # Creates instance of the MapReduceJob with configuration
    job = MapReduceJob(config)
    
    # Run job
    job.run()

if __name__ == "__main__":
    main()