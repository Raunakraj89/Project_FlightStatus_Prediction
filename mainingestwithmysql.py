import pyspark.sql as sql
import argparse
from ingestionmysql import doWork2 as doWork2MySQL
from ingestionbq import doWork2 as doWork2bq

from pyspark.sql.types import IntegerType

# def doWork():
#     spark = SparkSession.builder.appName("ClassAssignment2").getOrCreate()
#
#     bqDF = spark.read.format("bigquery").option("table","spring-asset-408702:hr.empdata").load()
#     bqDF2 = bqDF.withColumn("salaryasnum", bqDF["salary"].cast(IntegerType()))
#
#     sumDF = bqDF2.groupBy("dept_name").sum("salaryasnum")
#
#     sumDF.write.format("csv").mode("overwrite").save("gs://dataengg2023dec19/outputresults/sumsalbydept")
#
# def doWork2(inputTable, outputDirectory):
#     spark = SparkSession.builder.appName("ClassAssignment2").getOrCreate()
#
#     #bqDF = spark.read.format("bigquery").option("table", inputTable).load()
#     bqDF2 = bqDF.withColumn("salaryasnum", bqDF["salary"].cast(IntegerType()))
#
#     sumDF = bqDF2.groupBy("dept_name").sum("salaryasnum")
#
#     sumDF.write.format("csv").mode("overwrite").save(outputDirectory)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark DataFrame Assignment")
    parser.add_argument("input_table", help="Name of the BigQuery Table to read Data from")
    parser.add_argument("landing_dir", help="Path to the output directory")

    # Parse the arguments
    args = parser.parse_args()
    #doWork()
    spark = sql.SparkSession.builder.appName("Read MySQL Table").getOrCreate()

    #doWork2MySQL(spark, args.input_table, args.landing_dir)
    doWork2MySQL(spark, args.input_table, args.landing_dir)