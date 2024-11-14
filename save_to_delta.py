from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Configuração da sessão Spark com suporte ao Delta Lake
builder = SparkSession.builder \
    .appName("SaveDataFrameToDelta") \
    .config("spark.jars.packages", "io.delta:delta-core_2.13:2.1.0") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Criando um DataFrame de exemplo
data = [("Alice", 1), ("Bob", 2), ("Carol", 3)]
columns = ["Name", "Value"]
df = spark.createDataFrame(data, columns)

# Salvando o DataFrame em Delta no MinIO
output_path = "s3a://spark/delta_table"
df.write.format("delta").mode("overwrite").save(output_path)

print("DataFrame salvo com sucesso no MinIO em formato Delta.")
spark.stop()
