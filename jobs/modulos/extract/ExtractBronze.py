from pyspark.sql.functions import lit, col
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

from modulos.extract.extract import Extract
from modulos.configs.parametros import generate_bronze_layer_path

class ExtractBronze(Extract):
  
  def update_source_path(self):
    self.source_path = generate_bronze_layer_path(self.source_path)

  def assess_if_source_file_exists(self):
    path = self.source

    try:
      deltaTable = DeltaTable.forPath(self.spark_session, path)
      file_exists = True
    except AnalysisException:
      file_exists = False

    return file_exists

  def execute(self, extract_from_timestamp = None, **kwargs):
    source_file_status = self.assess_if_source_file_exists()

    if source_file_status:
      if self.mode == "delta":
        if extract_from_timestamp:
          print(f"|- Extracting entries after given timestamp {extract_from_timestamp} from source {self.source}")
        else:
          print(f"|- Extracting all entries from source {self.source}")

        df = self.extract(extract_from_timestamp)
      elif self.mode == "full":
        print(f"|- Extracting full batch from source {self.source}")
        df = self.extract(None)

      if self.alias:
        df = df.alias(self.alias)

    else:
      print(f"|- Source file coudn't be found on path '{self.source}'")
      df = None

    return df

  def extract_last_update():
    pass

  def extract(self, extract_from_timestamp):
    df = self.spark_session.read.format("delta").options(**self.options).load(self.source).select(
      "*", 
      col("_metadata.file_modification_time").alias("_file_modification_time"), 
      col("_metadata.file_path").alias("_file_path")
    )

    if extract_from_timestamp:
      filt__date = ( col("_import_timestamp")>extract_from_timestamp )

      df = df.filter( filt__date )

    print("Extracted dataframe with the following schema:")
    df.printSchema()

    return df