"""
Spark session helper + structured logging. In Databricks, `spark` is already injected into
the notebook globals — `get_spark()` simply returns the active session so the same code works
identically in notebooks, Jobs, and local pytest (with a local SparkSession for unit tests).
"""
import logging
import sys

from pyspark.sql import SparkSession


def get_spark(app_name: str = "sleepapnea_dp") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
