"""Format the logger"""
import logging

import yaml


def get_logger(name=str):
    """Getting a logger"""
    with open("app/logging_config.yaml", "r") as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(name)
    return logger
