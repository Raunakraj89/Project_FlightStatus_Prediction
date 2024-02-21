def dosummary(spark, stage_table, consume_table):
    project = "spring-asset-408702"
    stage_dataset = "stg"
    consume_dataset = "consumedataata"

    df = spark.read.format("bigquery").option("table","{}:{}.{}".format(project,stage_dataset,stage_table)).load()

    df.show()

    dfsummary = df.groupby("depname").sum("sal")

    dfsummary.show()
    dfsummary.write.format("bigquery").option("temporaryGcsBucket","tempbucketparesh").option("table", "{}:{}.{}".format(project, consume_dataset, consume_table)).mode("append").save()


