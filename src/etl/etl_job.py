"""
Python + Pyspark file loader
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This script allows user to load the file in specific format and write it back as parquet

"""


"""
etl_job.py
~~~~~~~~~~

This Python module contains an example Apache Spark ETL job definition
that implements best practices for production ETL jobs. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/etl_config.json \
    jobs/etl_job.py

where packages.zip contains Python modules required by ETL job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

For more details on submitting Spark applications, please see here:
http://spark.apache.org/docs/latest/submitting-applications.html

Our chosen approach for structuring jobs is to separate the individual
'units' of ETL - the Extract, Transform and Load parts - into dedicated
functions, such that the key Transform steps can be covered by tests
and jobs or called from within another environment (e.g. a Jupyter or
Zeppelin notebook).
"""

import pandas as pd
import numpy as np
from dependencies import logging
from dependencies import spark_factory
from pyspark.sql import functions as F

def main():
    """Main ETL script definition.

    :return: None
    """
    spark, log = spark_factory.session_setup('shell_challenge')


    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')

    # execute ETL pipeline
    energy_data = extract_data(
        spark, 
        file_path='src/etl/raw_data/ConsumptionData', 
        input_format='csv')
    
    energy_data_transformed = add_source_file_name(
        energy_data,
        extract_file_name_from_path=True)
    
    energy_data_transformed = fix_timestamp_column(energy_data)

    energy_data_transformed = convert_unit_types(energy_data)

    energy_data_transformed = quantise_to_hour(energy_data)

    meta_data = extract_data(spark, file_path='src/etl/raw_data/nmi_info.csv',input_format='csv')    
    
    meta_data_tranformed = add_source_file_name(meta_data, extract_file_name_from_path=False)

    energy_with_meta_data = energy_data_transformed.join(meta_data_tranformed, energy_data_transformed.nmi_id == meta_data_tranformed.Nmi, "left")
    
    energy_hourly_median = hourly_median(energy_with_meta_data)

    load_data(energy_hourly_median)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None

class EtlInput:

    def extract_data(spark, file_path="", input_format="csv") -> DataFrame:
        """Load data from any file format. This method can be extended to add any file formats in future
        :param spark: Spark session object.
        :param file_path: file path of the file either local or any remote location like S3.        
        :param input_format: file format of the input file
        :return: Spark DataFrame.
        """
        if input_format =="csv":
             df = (
                 spark
                 .read.csv(file_path, 
                           header=True, 
                           inferSchema=True)
                           )
             return df


def add_source_file_name(df, extract_file_name_from_path=False):
    """Transform original dataset.

    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """

    # add source file name

    df_transformed = (
        df
        .withColumn(
        'source_file_name', 
        F.input_file_name())
    )
    if extract_file_name_from_path:
        filename = F.udf(lambda x: x.rsplit('/',-1)[-1].split('.')[0])
        df_transformed = (
            df
            .withColumn(
            'nmi_id', 
            filename(df.source_file_name))
            )

    return df_transformed

def fix_timestamp_column(df):
    """
    Fixing the timestamp column, 
    The file  NMIG2.csv has timestamp format as dd/MM/yyyy HH:mm:ss while all other files has the format as yyyy-MM-dd HH:mm:ss
    This makes the entire AESTTime column to be treated as a string. Therefore converting it to genreric timestamp type

    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """

    df_fixed = (
        df
        .withColumn(
        'aest_time', 
        F.when(df.AESTTime.rlike('[0-9]{4}-[0-9]{2}-[0-9]{2}\s+[0-9]{2}:[0-9]{2}:[0-9]{2}'),
               F.to_timestamp('AESTTime','yyyy-MM-dd HH:mm:ss'))
               .when(df.AESTTime.rlike('[0-9]{2}\/[0-9]{2}\/[0-9]{4}\s+[0-9]{2}:[0-9]{2}:[0-9]{2}'),
                     F.to_timestamp('AESTTime','dd/MM/yyyy HH:mm:ss')))
    )
    return df_fixed

def convert_unit_types(df):
    """
    Fixing the timestamp column, 
    The file  NMIG2.csv has timestamp format as dd/MM/yyyy HH:mm:ss while all other files has the format as yyyy-MM-dd HH:mm:ss
    This makes the entire AESTTime column to be treated as a string. Therefore converting it to genreric timestamp type

    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """

    df_transformed = (
        df
        .withColumn(
        'quantity_kwh', 
        F.when (df.Unit =='Mwh', 
                df.Quantity * 1000)
                .otherwise(df.Quantity))
        .withColumn(
        'new_Unit', 
        F.when (df.Unit =='Mwh', 'Kwh')
        .otherwise(df.Unit))

    )
    return df_transformed

def quantise_to_hour(df):
    """
    Fixing the timestamp column, 
    The file  NMIG2.csv has timestamp format as dd/MM/yyyy HH:mm:ss while all other files has the format as yyyy-MM-dd HH:mm:ss
    This makes the entire AESTTime column to be treated as a string. Therefore converting it to genreric timestamp type

    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """

    df_transformed = (
        df
        .withColumn(
        'hour', 
        F.hour(df['aest_time']))

    )
    return df_transformed
                     

def hourly_median(df):
    """
    Fixing the timestamp column, 
    The file  NMIG2.csv has timestamp format as dd/MM/yyyy HH:mm:ss while all other files has the format as yyyy-MM-dd HH:mm:ss
    This makes the entire AESTTime column to be treated as a string. Therefore converting it to genreric timestamp type

    :param df: Input DataFrame.
    :param steps_per_floor_: The number of steps per-floor at 43 Tanner
        Street.
    :return: Transformed DataFrame.
    """

    hourly_median = (
        df
        .groupBy(F.col('hour'), 
                 F.col('nmi_id'), 
                 F.col('State'))
        .agg(F.median('quantity_kwh').alias('median_energy_consumption_per_hour'))

    )
    return hourly_median
                     
def load_data(df):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :return: None
    """
    (df
     .coalesce(1)
     .write
     .csv('loaded_data', mode='overwrite', header=True))
    return None


def create_test_data(spark, config):
    """Create test data.

    This function creates both both pre- and post- transformation data
    saved as Parquet files in tests/test_data. This will be used for
    unit tests as well as to load as part of the example ETL job.
    :return: None
    """
    # create example data from scratch
    local_records = [
        Row(id=1, first_name='Dan', second_name='Germain', floor=1),
        Row(id=2, first_name='Dan', second_name='Sommerville', floor=1),
        Row(id=3, first_name='Alex', second_name='Ioannides', floor=2),
        Row(id=4, first_name='Ken', second_name='Lai', floor=2),
        Row(id=5, first_name='Stu', second_name='White', floor=3),
        Row(id=6, first_name='Mark', second_name='Sweeting', floor=3),
        Row(id=7, first_name='Phil', second_name='Bird', floor=4),
        Row(id=8, first_name='Kim', second_name='Suter', floor=4)
    ]

    df = spark.createDataFrame(local_records)

    # write to Parquet file format
    (df
     .coalesce(1)
     .write
     .parquet('tests/test_data/employees', mode='overwrite'))

    # create transformed version of data
    df_tf = transform_data(df, config['steps_per_floor'])

    # write transformed version of data to Parquet
    (df_tf
     .coalesce(1)
     .write
     .parquet('tests/test_data/employees_report', mode='overwrite'))

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()