from configparser import ConfigParser
from tester import Tester

# from config.logger import Logger, setup_logger
import os
import logging


def initialize_log(logging_level):
    """
    Python custom logging initialization

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging_level,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Disable Pika library logs
    logging.getLogger("pika").setLevel(logging.WARNING)


def initialize_config():
    """Parse env variables or config file to find program config params

    Function that search and parse program configuration parameters in the
    program environment variables first and the in a config file.
    If at least one of the config parameters is not found a KeyError exception
    is thrown. If a parameter could not be parsed, a ValueError is thrown.
    If parsing succeeded, the function returns a ConfigParser object
    with config parameters
    """

    config = ConfigParser(os.environ)
    # If config.ini does not exists original config object is not modified
    config.read("config.ini")

    config_params = {}
    try:
        config_params["port"] = int(
            os.getenv("SERVER_PORT", config["DEFAULT"]["SERVER_PORT"])
        )
        config_params["listen_backlog"] = int(
            os.getenv(
                "SERVER_LISTEN_BACKLOG", config["DEFAULT"]["SERVER_LISTEN_BACKLOG"]
            )
        )
        config_params["logging_level"] = os.getenv(
            "LOGGING_LEVEL", config["DEFAULT"]["LOGGING_LEVEL"]
        )
    except KeyError as e:
        error_msg = f"Key was not found. Error: {e}. Aborting server"
        raise KeyError(error_msg)
    except ValueError as e:
        error_msg = f"Key could not be parsed. Error: {e}. Aborting server"
        raise ValueError(error_msg)

    return config_params


def main():
    try:
        config_params = initialize_config()
        logging_level = config_params["logging_level"]
        port = config_params["port"]
        listen_backlog = config_params["listen_backlog"]

        initialize_log(logging_level)

        # Log config parameters at the beginning of the program to verify the configuration
        # of the component
        logging.debug(
            f"action: config | result: success | port: {port} | "
            f"listen_backlog: {listen_backlog} | logging_level: {logging_level}"
        )

        # Initialize tester and start tester loop
        tester = Tester(port, listen_backlog)
        tester.run()
    except Exception as e:
        logging.exception("Uncaught exception")
        raise


if __name__ == "__main__":
    main()
