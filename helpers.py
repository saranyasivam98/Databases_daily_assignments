# -- coding: UTF-8 --
"""Helper functions"""

import json
import logging
import logging.config
import argparse
import functools
import time


__author__ = 'saranya@gyandata.com'

LOGGER = logging.getLogger(__name__)
LOGGER_CONFIG_PATH = 'config/logging.json'


def setup_logging(default_path=LOGGER_CONFIG_PATH):
    """
   Function Description: To setup logging using the json file
   :param default_path: Path of the configuration file
   :type default_path: str
  """
    with open(default_path, 'rt') as file:
        config = json.load(file)
    logging.config.dictConfig(config)


def arg_parse():
    """
  Function Description: To parse command line arguments

  :return: command line arguments passed
  """

    parser = argparse.ArgumentParser()
    parser.add_argument('-m', help="model name", type=str)
    return parser.parse_args()


def timer(func):
    """Print the runtime of the decorated function"""
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()    # 1
        value = func(*args, **kwargs)
        end_time = time.perf_counter()      # 2
        run_time = end_time - start_time    # 3
        LOGGER.info("Finished %s in %f secs", func.__name__, run_time)
        return value
    return wrapper_timer
