from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

def get_list_files(spark:SparkSession, bucket: str, prefix: str) -> list:
    hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = spark._jvm.org.apache.hadoop.fs.Path(f"s3a://{bucket}/{prefix}")
    files_status = hadoop_fs.get(path.toUri(), hadoop_conf).listStatus(path)
    return [f.getPath().toString() for f in files_status if f.isFile()]

def fix_schemas(df: DataFrame, fixed_columns: list) -> DataFrame:

    for column_name, column_type in fixed_columns:
        if column_name not in df.columns:
            df = df.withColumn(column_name, lit(None).cast(column_type))
        else:
            df = df.withColumn(column_name, df[column_name].cast(column_type))
    
    return df.select([col_name for col_name, _ in fixed_columns])