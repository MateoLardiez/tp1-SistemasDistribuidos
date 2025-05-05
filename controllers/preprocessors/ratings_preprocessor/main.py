from preprocessor import RatingsPreprocessor
import logging
import os

def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging_level,
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    
    # Disable Pika library logs
    logging.getLogger('pika').setLevel(logging.WARNING)

def main():
    numberWorkers = int(os.getenv("N_WORKERS", 1))
    initialize_log("INFO")

    preprocessorRatings = RatingsPreprocessor(numberWorkers)
    preprocessorRatings.start()
    
if __name__ == "__main__":
    main()