def doenrich(spark, input_table, enriched_table):
    project = "spring-asset-408702"
    dataset = "stg"
    reference_dataset = "referencedata"
    reference_table = "dept"

    df = spark.read.format("bigquery").option("table","{}:{}.{}".format(project,dataset,input_table)).load()

    df.show()

    dfreferencedata = spark.read.format("bigquery").option("table","{}:{}.{}".format(project,reference_dataset,reference_table)).load()

    dfreferencedata.show()


    dfenriched = df.join(dfreferencedata,df.dep_id == dfreferencedata.depid,"inner")
    dfenriched.show()

    #dfclean = dfenriched.drop("depid")

    dffinal = dfenriched.select(dfenriched.depid, dfenriched.depname, dfenriched.sal)

    dffinal.show()
    dffinal.write.format("bigquery").option("temporaryGcsBucket","tempbucketparesh").option("table", "{}:{}.{}".format(project, dataset, enriched_table)).mode("append").save()

    #dfclean.write.format("bigquery").option("table", "spring-asset-408702:stg.clean_data").option("temporaryGcsBucket","gs://tempbucketparesh").option("mode",                                                                                                  "append").save()

