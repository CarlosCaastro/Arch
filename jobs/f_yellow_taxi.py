from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, col,  sum, hash, abs
from pyspark.sql.utils import AnalysisException
from pyspark.sql import Window
import re

from delta.tables import DeltaTable
from modulos.extract.ExtractDelta import ExtractDelta
from modulos.configs.parametros import URL_POSTGRE, PROPERTIES_POSTGRE
from modulos.load.LoadDelta import LoadDelta

spark = SparkSession.builder \
    .appName("Fato Yellow_Taxi") \
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

extract = ExtractDelta(
    source_path="yellow_taxi",
    source_name="/silver_yewllow_taxi",
    mode="full",
    layer='silver',
    file_format="delta").SetSparkSession(spark_session=spark)

df = extract.execute()
df_filtred = df.select("VENDORID", "PASSENGER_COUNT", "TOTAL_AMOUNT", "TPEP_PICKUP_DATETIME", "TPEP_DROPOFF_DATETIME")

df_bef_sk = df_filtred.withColumn("DAY", dayofmonth(col("TPEP_PICKUP_DATETIME"))) \
               .withColumn("MONTH", month(col("TPEP_PICKUP_DATETIME"))) \
               .withColumn("YEAR", year(col("TPEP_PICKUP_DATETIME"))) \
               .withColumn("HOUR", hour(col("TPEP_PICKUP_DATETIME")))

agg_df = df_bef_sk.groupBy("VENDORID", "YEAR", "MONTH", "DAY","HOUR") \
           .agg(
               sum("PASSENGER_COUNT").alias("TOTAL_PASSENGERS"),
               sum("TOTAL_AMOUNT").alias("TOTAL_AMOUNT")
           )

df_with_sk = agg_df.withColumn("SK_CALENDAR", abs(hash(col("YEAR"), col("MONTH"), col("DAY"), col("HOUR"))).cast("bigint"))

df_f_yellow = df_with_sk.select("VENDORID","SK_CALENDAR", "TOTAL_PASSENGERS", "TOTAL_AMOUNT")

load = LoadDelta(
    sink_path="f_yellow_taxi/", 
    sink_name="f_yellow_taxi", 
    keys="VENDORID,SK_CALENDAR", 
    file_format="delta",
    layer="gold").SetSparkSession(spark_session=spark).SetDataframe(df=df_f_yellow)

load.execute()

df_f_yellow.write.jdbc(url=URL_POSTGRE, table="gold.f_yellow_taxi", mode="overwrite", properties=PROPERTIES_POSTGRE)

spark.stop()
