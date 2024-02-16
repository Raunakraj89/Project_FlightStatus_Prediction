def doenrichdqcheck(spark, input_table, enriched_table):
    project = "spring-asset-408702"
    dataset = "stg"
    reference_dataset = "referencedata"
    reference_table = "dept"


    stg_count = spark.read.format("bigquery").option("table","{}:{}.{}".format(project,dataset,input_table)).load().count()
    print(stg_count)

    enrich_count = spark.read.format("bigquery").option("table","{}:{}.{}".format(project,dataset,enriched_table)).load().count()

    if stg_count == enrich_count:
        print("DQ check passed")
    else:
        print("DQ check failed")
        raise Exception("DQ check failed")

    #dfclean.write.format("bigquery").option("table", "spring-asset-408702:stg.clean_data").option("temporaryGcsBucket","gs://tempbucketparesh").option("mode",                                                                                                  "append").save()

