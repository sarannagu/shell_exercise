"""
test_etl_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest

import json

from pyspark.sql.functions import mean

from dependencies import spark_factory
from jobs.etl_job import add_source_file_name,fix_timestamp_column, convert_unit_types, quantise_to_hour, hourly_median


class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.config = json.loads("""{"steps_per_floor": 21}""")
        self.spark, *_ = spark_factory.session_setup('test_etl_job')
        self.test_data_path = 'tests/test_data/'

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_transformers(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble
        input_data = (
            self.spark
            .read
            .csv(self.test_data_path + 'employees'))

        expected_data = (
            self.spark
            .read
            .csv(self.test_data_path, header=True, inferSchema=True)
        )

        expected_cols = len(expected_data.columns)
        expected_rows = expected_data.count()

        # add source filename column and check if that column exists
        data_transformed = add_source_file_name(input_data)
        self.assertEqual('source_file_name' in data_transformed.columns)
        
        data_transformed = fix_timestamp_column(input_data)
        self.assertEqual('source_file_name' in data_transformed.columns)

        # check if all of the new units are in kwh, so only 1 distinct value of the column quantity_kwh
        data_transformed = convert_unit_types(input_data)
        self.assertEqual (data_transformed.select('quantity_kwh').distinct().count() == 1 )
        
        
        data_transformed = quantise_to_hour(input_data)
        self.assertEqual('hour' in data_transformed.columns)

        # finding the median usage of energy over the multiple days, this means there are 12 sites and 24 hours a day, 
        # which means there would be 288 records in this dataframe
        data_transformed = hourly_median(input_data)
        self.assertEqual(data_transformed.count() == 288)
        
        cols = len(expected_data.columns)
        rows = expected_data.count()
        avg_steps = (
            expected_data
            .agg(mean('steps_to_desk').alias('avg_steps_to_desk'))
            .collect()[0]
            ['avg_steps_to_desk'])

        # assert
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)


if __name__ == '__main__':
    unittest.main()