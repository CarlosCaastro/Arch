from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofmonth, month, year, hour, expr, concat_ws, hash, abs

from modulos.load.LoadDelta import LoadDelta
from modulos.extract.ExtractDelta import ExtractDelta
from modulos.configs.parametros import URL_POSTGRE, PROPERTIES_POSTGRE

spark = SparkSession.builder \
    .appName("Dimension Calendar") \
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


df = spark.sql("SELECT sequence(to_date('2023-01-01'), to_date('2023-12-31'), interval 1 day) AS dates")
df = df.selectExpr("explode(dates) AS data")

hours_df = spark.sql("SELECT sequence(0, 23) AS hours")
hours_df = hours_df.selectExpr("explode(hours) AS hour")

df = df.crossJoin(hours_df)

df = df.withColumn("DAY", dayofmonth("data")) \
       .withColumn("MONTH", month("data")) \
       .withColumn("YEAR", year("data")) \
       .withColumn("HOUR", col("hour"))

df = df.withColumn("SK_CALENDAR", abs(hash(col("YEAR"), col("MONTH"), col("DAY"), col("HOUR"))).cast("bigint"))

df = df.select("SK_CALENDAR", "HOUR", "DAY", "MONTH", "YEAR")

load = LoadDelta(
    sink_path="d_calendar/", 
    sink_name="d_calendar", 
    keys="SK_CALENDAR", 
    file_format="delta",
    layer="gold").SetSparkSession(spark_session=spark).SetDataframe(df=df)

load.execute()

df.write.jdbc(url=URL_POSTGRE, table="gold.d_calendar", mode="overwrite", properties=PROPERTIES_POSTGRE)

spark.stop()