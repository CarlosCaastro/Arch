from abc import ABC, abstractmethod
from pyspark.sql.functions import lit
from datetime import datetime

class Load(ABC):
    def __init__(self, sink_path:str, sink_name:str, keys:str, mode:str="overwrite", file_format:str="delta", options:dict={}, layer:str=None,catalog:str=None, schema:str=None, table_suffix:str=None):
      self.layer = layer
      self.sink_path = sink_path
      self.update_sink_path()
      self.sink_name = sink_name
      self.keys = keys.split(",")
      self.mode = mode
      self.file_format = file_format
      self.options = options
      self.sink = self.sink_path + self.sink_name
      self.control_table = f"{self.sink_path}_control_table"
      self.catalog = self.set_catalog(catalog)
      self.schema = self.set_schema(schema)
      self.table = self.set_table(table_suffix)

    def SetDataframe(self, df):
      if df:
        if "_timestamp" not in df.columns:
          timestamp_value = self.extract_current_timestamp(timestamp_format="unix")
          print(f"| - Appending time stamp value to the dataframe: {timestamp_value}")
          self.dataframe = df.withColumn(
            "_timestamp", lit(timestamp_value)
          )
        else:
          self.dataframe = df
      else:
        self.dataframe = None

      return self
    
    def SetSparkSession(self, spark_session):
      self.spark_session = spark_session

      return self

    def extract_current_timestamp(self, timestamp_format="unix"):
      now = datetime.now()
      if timestamp_format == "unix":
        timestamp = int(datetime.timestamp(now)*1000)
      else:
        timestamp =  datetime.strftime(now, timestamp_format)

      return timestamp

    @abstractmethod
    def execute(self, **kwargs):
      return self
    
    @abstractmethod
    def extract_last_update(self, **kwargs):
      return self
  
    @abstractmethod
    def update_sink_path(self,**kwargs):
      return self
  
    @abstractmethod
    def set_catalog(self,**kwargs):
      return self
    
    @abstractmethod
    def set_schema(self,**kwargs):
      return self
  
    @abstractmethod
    def set_table(self,**kwargs):
      return self
    
