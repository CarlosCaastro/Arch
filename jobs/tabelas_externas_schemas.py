from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Create External Tables for Delta Format") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("javax.jdo.option.ConnectionURL", "jdbc:postgresql://postgres:5432/airflow") \
    .config("javax.jdo.option.ConnectionUserName", "airflow") \
    .config("javax.jdo.option.ConnectionPassword", "airflow") \
    .enableHiveSupport() \
    .getOrCreate()

schemas = ["bronze", "silver", "gold"]
for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"Schema '{schema}' criado com sucesso.")


drop_queries = {
    "bronze.yellow_taxi": """
        DROP TABLE IF EXISTS bronze.yellow_taxi;
    """,
    "silver.yellow_taxi": """
        DROP TABLE IF EXISTS silver.yellow_taxi;
    """,
    "gold.f_yellow_taxi": """
        DROP TABLE IF EXISTS gold.f_yellow_taxi;
    """,
    "gold.d_calendar": """
        DROP TABLE IF EXISTS gold.d_calendar;
    """
}

tables_queries = {
    "bronze.yellow_taxi": """
        CREATE EXTERNAL TABLE IF NOT EXISTS bronze.yellow_taxi
        USING delta
        LOCATION 's3a://ifood/bronze/yellow_taxi/bronze_yellow_taxi'
    """,
    "silver.yellow_taxi": """
        CREATE EXTERNAL TABLE IF NOT EXISTS silver.yellow_taxi
        USING delta
        LOCATION 's3a://ifood/silver/yellow_taxi/silver_yewllow_taxi'
    """,
    "gold.f_yellow_taxi": """
        CREATE EXTERNAL TABLE IF NOT EXISTS gold.f_yellow_taxi
        USING delta
        LOCATION 's3a://ifood/gold/f_yellow_taxi/f_yellow_taxi'
    """,
    "gold.d_calendar": """
        CREATE EXTERNAL TABLE IF NOT EXISTS gold.d_calendar
        USING delta
        LOCATION 's3a://ifood/gold/d_calendar/d_calendar'
    """
}

for table, query in drop_queries.items():
    spark.sql(query)
    print(f"Tabela externa '{table}' dropada.")

for table, query in tables_queries.items():
    spark.sql(query)
    print(f"Tabela externa '{table}' criada com sucesso.")

spark.stop()

