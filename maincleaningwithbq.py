import pyspark.sql as sql
import argparse
from datacleaning import doclean

from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark DataFrame Assignment")
    parser.add_argument("input_gcsfile", help="Name of the BigQuery Table to read Data from")
    parser.add_argument("bq_table", help="Path to the output directory")

    # Parse the arguments
    args = parser.parse_args()
    #doWork()
    spark = sql.SparkSession.builder.appName("Clean Data").getOrCreate()

    #doWork2MySQL(spark, args.input_table, args.landing_dir)
    doclean(spark, args.input_gcsfile, args.bq_table)