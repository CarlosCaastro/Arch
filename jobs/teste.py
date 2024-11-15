from pyspark.sql import SparkSession
from delta import *

# Configuração do Spark para Delta e MinIO
spark = SparkSession.builder \
    .appName("DeltaWithMinIO") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.warehouse.dir", "/opt/bitnami/spark/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()
    #
# Caminho no MinIO para os dados Delta
delta_table_path = "s3a://spark/delta/tabela_teste_3"

# Escreve dados no formato Delta no MinIO
data = spark.range(0, 5)
#data.write.format("delta").mode("overwrite").save(delta_table_path)
data.write.format("delta").option("path","s3a://spark/delta/tabela_teste_2").mode("overwrite").saveAsTable("teste_table_3")

# Criação da tabela EXTERNAL no Hive Metastore
# spark.sql(f"""
# CREATE EXTERNAL TABLE IF NOT EXISTS default.tabela_2 (
#     id BIGINT
# )
# USING delta
# LOCATION '{delta_table_path}'
# """)

print("Tabela registrada no Hive Metastore com sucesso!")
