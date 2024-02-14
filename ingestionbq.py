def doWork2(spark, inputTable, landingDirectory):
    # Get the credentials of the current service account
    project = "spring-asset-408702"
    dataset = "hr"


    # Create a SparkSession

    # Create a JDBC connection to the MySQL database

    # Read the data from the MySQL table
    # df = spark.read.format("bigquery").option("table","spring-asset-408702:hr.emp").load()
    df = spark.read.format("bigquery").option("table","{}:{}.{}".format(project,dataset,inputTable)).load()
    # Display the data
    df.show()

    df.write.format("csv").mode("overwrite").option("header", "true").save(landingDirectory)