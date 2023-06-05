"""
spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""

import __main__

from pyspark.sql import SparkSession

from dependencies import logging


def session_setup(app_name='my_spark_app', master='local[*]', jar_packages=[],
                files=[], spark_config={}) :
    
    """Start Spark session, get Spark logger and load config files."""

    spark = (   
         SparkSession.builder
         .appName(app_name)
         .getOrCreate()
         )
    log = logging.Log4j(spark=spark)
    
    return spark, log