def doenrich(spark, input_table, enriched_table):
    project = "spring-asset-408702"
    dataset = "stg"
    reference_dataset = "referencedata"
    reference_table = "dept"


    df_emp_stg_original = spark.read.format("bigquery").option("table","{}:{}.{}".format(project,dataset,input_table)).load()

    df_emp_stg = df_emp_stg_original.withColumnRenamed("depid", "dep_id")
    df_emp_stg.show()

    df_ref_dept = spark.read.format("bigquery").option("table","{}:{}.{}".format(project,reference_dataset,reference_table)).load()

    df_ref_dept.show()


    df_emp_enriched = df_emp_stg.join(df_ref_dept,df_emp_stg.dep_id == df_ref_dept.depid,"inner")
    df_emp_enriched.show()

    #dfclean = dfenriched.drop("depid")

    dffinal_1 = df_emp_enriched.select(df_emp_enriched.depid, df_emp_enriched.depname, df_emp_enriched.salary)
    dffinal = dffinal_1.withColumnRenamed("salary", "sal")

    dffinal.show()
    dffinal.write.format("bigquery").option("temporaryGcsBucket","tempbucketparesh").option("table", "{}:{}.{}".format(project, dataset, enriched_table)).mode("overwrite").save()

    #dfclean.write.format("bigquery").option("table", "spring-asset-408702:stg.clean_data").option("temporaryGcsBucket","gs://tempbucketparesh").option("mode",                                                                                                  "append").save()

