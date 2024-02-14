def doclean(spark, input_gcsfile, bq_table):
    df = spark.read.format("csv").option("path", input_gcsfile).option("header", "true").option("inferSchema", "true").load()

    df.show()
    #dfclean = df.select("dep", "arr").filter(df.dep != "")