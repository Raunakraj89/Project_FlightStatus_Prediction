import pyspark.sql as sql
import argparse
from dataenrich import (doenrich)

from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark DataFrame Assignment")
    parser.add_argument("emp_stg_table", help="Name of the BigQuery Table to read Data from")
    parser.add_argument("emp_enriched_table", help="Path to the output directory")

    # Parse the arguments
    args = parser.parse_args()
    #doWork()
    spark = sql.SparkSession.builder.appName("Enrich Data").getOrCreate()

    #doWork2MySQL(spark, args.input_table, args.landing_dir)
    doenrich(spark, args.emp_stg_table, args.emp_enriched_table)