from aggregator import AggregatorRB
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
    number_workers = int(os.getenv("N_WORKERS"))
    id_worker = int(os.getenv("WORKER_ID"))
    initialize_log("INFO")

    aggregator = AggregatorRB(number_workers, id_worker)
    aggregator.start()
    
if __name__ == "__main__":
    main()