from health_checker import HealthChecker
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

def main():
    interval = int(os.getenv("HEALTH_CHECK_INTERVAL"))
    timeout = int(os.getenv("HEALTH_CHECK_TIMEOUT"))
    num_healthcheck = int(os.getenv("N_HEALTHCHECKERS"))
    id_healthchecker = os.getenv("HEALTH_CHECKER_ID")
    initialize_log("INFO")

    healtchecker = HealthChecker(
        health_check_interval=interval,
        health_check_timeout=timeout,
        controller_id=id_healthchecker,
        num_of_healthcheckers=num_healthcheck
    )
    healtchecker.start()
    
if __name__ == "__main__":
    main()