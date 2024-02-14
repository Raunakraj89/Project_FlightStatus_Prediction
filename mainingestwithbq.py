import pyspark.sql as sql
import argparse
#from ingestionmysql import doWork2 as doWork2MySQL
from ingestionbq import doWork2 as doWork2bq

from pyspark.sql.types import IntegerType




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark DataFrame Assignment")
    parser.add_argument("input_table", help="Name of the BigQuery Table to read Data from")
    parser.add_argument("landing_dir", help="Path to the output directory")

    # Parse the arguments
    args = parser.parse_args()
    #doWork()
    spark = sql.SparkSession.builder.appName("Read BQ Table").getOrCreate()

    #doWork2MySQL(spark, args.input_table, args.landing_dir)
    doWork2bq(spark, args.input_table, args.landing_dir)