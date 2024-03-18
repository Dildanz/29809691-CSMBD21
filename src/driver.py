import yaml
from main_job import MapReduceJob

def main():
    with open('config.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)
    job = MapReduceJob(config)
    job.run()

if __name__ == "__main__":
    main()