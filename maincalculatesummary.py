import pyspark.sql as sql
import argparse
from calculatesummary import (dosummary)

from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark DataFrame Assignment")
    parser.add_argument("stage_table", help="Name of the BigQuery Table to read Data from")
    parser.add_argument("consume_table", help="Path to the output directory")

    # Parse the arguments
    args = parser.parse_args()
    #doWork()
    spark = sql.SparkSession.builder.appName("Calculate Summary").getOrCreate()

    #doWork2MySQL(spark, args.input_table, args.landing_dir)
    dosummary(spark, args.stage_table, args.consume_table)