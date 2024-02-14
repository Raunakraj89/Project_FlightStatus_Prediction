from utilities import getsecret

def doWork2(spark, inputTable, landingDirectory):
    # Get the credentials of the current service account
    project = "906962432092"
    secretid = "mysqlrootpassword"
    mypassword = "abcd1234" # getsecret(project, secretid)
    print("I was able to get the data")
    myuser = "root"

    # Create a SparkSession

    # Create a JDBC connection to the MySQL database
    jdbcUrl = "jdbc:mysql://34.42.22.44:3306/hr_oltp"  # "jdbc:mysql://10.0.112.3:3306/hr_oltp"
    connectionProperties = {"user": myuser, "password": mypassword, "driver": "com.mysql.jdbc.Driver"}

    # Read the data from the MySQL table
    df = spark.read.jdbc(jdbcUrl, inputTable, properties=connectionProperties)

    # Display the data
    df.show()

    df.write.format("csv").mode("overwrite").save(landingDirectory)