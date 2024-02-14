def doclean(spark, input_gcsfile, bq_table):
    project = "spring-asset-408702"
    dataset = "stg"

    df = spark.read.format("csv").option("path", input_gcsfile).option("header", "true").option("inferSchema", "true").load()

    df.show()
    dfclean = df.select("empid", "depid", "salary").filter(df.depid.isNotNull())
    dfclean.show()
    dfclean.write.format("bigquery").option("temporaryGcsBucket","tempbucketparesh").option("table", "{}:{}.{}".format(project, dataset, bq_table)).mode("append").save()

    #dfclean.write.format("bigquery").option("table", "spring-asset-408702:stg.clean_data").option("temporaryGcsBucket","gs://tempbucketparesh").option("mode",                                                                                                  "append").save()

