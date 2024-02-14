import pyspark.sql as sql
import argparse
from dataenrich import (doenrich)

from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark DataFrame Assignment")
    parser.add_argument("input_table", help="Name of the BigQuery Table to read Data from")
    parser.add_argument("enriched_table", help="Path to the output directory")

    # Parse the arguments
    args = parser.parse_args()
    #doWork()
    spark = sql.SparkSession.builder.appName("Read GCS CSV file").getOrCreate()

    #doWork2MySQL(spark, args.input_table, args.landing_dir)
    doenrich(spark, args.input_table, args.enriched_table)