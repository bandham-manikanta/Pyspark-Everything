def spark_workflow():
    import findspark
    findspark.init()

    from pyspark.sql import SparkSession

    log4j2_path = "file:///home/bandham/airflow/log4j2.properties"

    spark = SparkSession.builder.appName('PySpark MySQL Connection') \
        .config("spark.driver.extraJavaOptions", f"-Dlog4j.configurationFile={log4j2_path}") \
        .config("spark.executor.extraJavaOptions", f"-Dlog4j.configurationFile={log4j2_path}") \
        .getOrCreate()
    # .config('spark.jars',r"C:\Users\BANDHAM\AppData\Roaming\DBeaverData\drivers\maven\maven-central\com.mysql\mysql-connector-j-8.2.0.jar")\

    jdbc_url = 'jdbc:mysql://10.88.0.2:3306/mysql'
    
    connection_properties = {
        "user":"root",
        "password":"strong_password",
        "driver":"com.mysql.cj.jdbc.Driver",
        "allowPublicKeyRetrieval": "true"
    }

    # Sample data
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]

    # Define schema
    columns = ["Name", "Age"]

    # print(spark)

    # Create DataFrame
    df = spark.createDataFrame(data, schema=columns)

    result_table_name = "bandham.PERSON"

    # Reading no of rows in the table.
    count_query = "(SELECT COUNT(*) AS row_count FROM {}) AS count_query".format(result_table_name)
    table_noof_rows_before = spark.read.jdbc(url=jdbc_url, table=count_query, properties=connection_properties)
    row_count_before = table_noof_rows_before.collect()[0]['row_count']
    print('Nof of rows before inserting:', row_count_before, end='\n\n')

    # Insert data into mysql
    df.write.jdbc(jdbc_url, result_table_name, mode="append", properties=connection_properties)

    table_noof_rows_after = spark.read.jdbc(url=jdbc_url, table=count_query, properties=connection_properties)
    row_count_after = table_noof_rows_after.collect()[0]['row_count']
    print('Nof of rows after inserting:', row_count_after, end='\n\n')

    print('Successfully saved {} rows into MySQL database.'.format(row_count_after - row_count_before), end='\n\n')

spark_workflow()