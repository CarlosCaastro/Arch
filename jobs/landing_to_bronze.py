from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from modulos.configs.parametros import BUCKET_NAME, FIXED_SCHEMA_INGESTION, URL_POSTGRE, PROPERTIES_POSTGRE
from modulos.utils.functions import get_list_files, fix_schemas
from modulos.load.LoadDelta import LoadDelta

spark = SparkSession.builder \
    .appName("Landing to Bronze") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", "s3a://ifood/warehouse") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("javax.jdo.option.ConnectionURL", "jdbc:postgresql://postgres:5432/airflow") \
    .config("javax.jdo.option.ConnectionUserName", "airflow") \
    .config("javax.jdo.option.ConnectionPassword", "airflow") \
    .enableHiveSupport() \
    .getOrCreate()


file_paths = get_list_files(spark , BUCKET_NAME, "landing/yellow_taxi_files")

dataframes = []

for path in file_paths:
    df = spark.read.parquet(path)
    
    if "airport_fee" in df.columns:
        df = df.withColumnRenamed("airport_fee", "Airport_fee")
    
    df = fix_schemas(df, FIXED_SCHEMA_INGESTION)
    dataframes.append(df)

final_df = dataframes[0]
for df in dataframes[1:]:
    final_df = final_df.unionByName(df)

load = LoadDelta(
    sink_path="yellow_taxi/", 
    sink_name="bronze_yellow_taxi", 
    keys="VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,improvement_surcharge,tolls_amount,passenger_count,trip_distance,PULocationID,total_amount", 
    file_format="delta",
    layer='bronze').SetSparkSession(spark_session=spark).SetDataframe(df=final_df)

load.execute()
load.update_control_table(source_name="yellow_taxi_files", source_path="s3a://{BUCKET_NAME}/landing/yellow_taxi_files")

final_df.write.jdbc(url=URL_POSTGRE, table="bronze.yellow_taxi", mode="overwrite", properties=PROPERTIES_POSTGRE)

spark.stop()

