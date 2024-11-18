from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, desc, row_number, when, lit, max
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Window
import re

from delta.tables import DeltaTable
from modulos.extract.ExtractDelta import ExtractDelta
from modulos.load.LoadDelta import LoadDelta
from modulos.configs.parametros import URL_POSTGRE, PROPERTIES_POSTGRE

spark = SparkSession.builder \
    .appName("Bronze to Silver") \
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

extract = ExtractDelta(source_path="yellow_taxi",source_name="/bronze_yellow_taxi",mode="full",file_format="delta", layer='bronze').SetSparkSession(spark_session=spark)

df = extract.execute()

def sanitize_input(df, output_keys):
  input_columns = df.columns
  separator_regex = r',\s*|\s*,\s*'
  keys_list = re.split(separator_regex,output_keys)
  window = Window.partitionBy(*keys_list).orderBy(desc(col("_file_modification_time")))

  select_cols = []
  for column in input_columns:
    if "_file_" not in column:
      if "." in column:
        column_1 = f'`{column}`'
        select_instance = (
          when(
            ( length( col(column_1) ) == 0 ), lit(None)
          ).otherwise( col(column_1) ) 
        ).alias(column.upper())

      else:  
        select_instance = (
          when(
            ( length( col(column) ) == 0 ), lit(None)
          ).otherwise( col(column) ) 
        ).alias(column.upper())
      select_cols.append(select_instance)
  sanitized_df = df.withColumn("__rn__", row_number().over(window)).filter(col("__rn__")==1).select(*select_cols)
  return sanitized_df

sanitized_df = sanitize_input(df, "VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,improvement_surcharge,tolls_amount,passenger_count,trip_distance,PULocationID,total_amount")

load = LoadDelta(
    sink_path="yellow_taxi/", 
    sink_name="silver_yewllow_taxi", 
    keys="VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,improvement_surcharge,tolls_amount,passenger_count,trip_distance,PULocationID,total_amount", 
    file_format="delta",
    layer='silver').SetSparkSession(spark_session=spark).SetDataframe(df=sanitized_df)

load.execute()
load.update_control_table(source_name="yellow_taxi_files", source_path="s3a://{BUCKET_NAME}/landing/yellow_taxi_files")

spark.stop()