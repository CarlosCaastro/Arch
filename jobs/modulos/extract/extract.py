from abc import ABC, abstractmethod
from datetime import datetime

class Extract(ABC):
    def __init__(self, source_path:str, source_name:str, mode:str="full", file_format="delta", options:dict={}, layer:str=None,alias:str=None):
        self.layer = layer
        self.source_path = source_path
        self.source_name = source_name
        self.update_source_path()
        self.mode = mode
        self.file_format = file_format
        self.options = options
        self.alias = alias
        self.source = self.source_path + self.source_name

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
      pass

    @abstractmethod
    def extract_last_update(self,**kwargs):
      return self
  
    @abstractmethod
    def update_source_path(self,**kwargs):
      return self
