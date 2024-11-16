from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
from modulos.load.load import Load
from modulos.configs.parametros import generate_silver_layer_path
from datetime import datetime

class LoadSilver(Load):
    def assess_if_target_file_exists(self):
        try:
            deltaTable = DeltaTable.forPath(self.spark_session, self.sink)
            file_exists = True
        except AnalysisException:
            file_exists = False

        return file_exists
    
    def update_sink_path(self):
        self.sink_path = generate_silver_layer_path(output_path=self.sink_path)

    def execute(self):
        output_file_path = self.sink

        target_file_status = self.assess_if_target_file_exists()

        if self.dataframe:
            if target_file_status:
                self.merge_output()
                print("Merge complete!")
            else:
                self.create_output()
                print("Creation complete!")
        else:
            print("No data to be loaded!")

    def create_output(self):
        self.dataframe.write.mode("overwrite").format(self.file_format).options(**self.options).save(self.sink)
        self.create_table_in_catalog()

    def create_table_in_catalog(self):
        #Script para criação da tabela no catalog desejado, executar com spark.sql, 
        #Formato CREATE EXTERNAL TABLE IF NOT EXIST <TABLE> USING DELTA LOCATION <self.sink>
        pass

    def merge_output(self):
        join_clause = self.get_join_clause()
        set_fields_insert, set_fields_update = self.evaluate_field_mapping()

        deltaTable = DeltaTable.forPath(self.spark_session, self.sink)

        deltaTable.alias("dfOld").merge(self.dataframe.alias("dfNew"), join_clause).whenMatchedUpdate(set = set_fields_update).whenNotMatchedInsert(values = set_fields_insert).execute()        


    def get_join_clause(self):
        keys_join = []
        
        for key in self.keys:
            key_instance = f"dfOld.`{key}` = dfNew.`{key}`"
            keys_join.append(key_instance)
        
        join_clause = " AND ".join(keys_join)

        return (join_clause)

    def evaluate_field_mapping(self):
        field_map_insert = {}
        field_map_update = {}

        for column in self.dataframe.columns:
            if "_timestamp" not in column:
                field_map_insert[f"`{column}`"] = f"dfNew.`{column}`"
                field_map_update[f"`{column}`"] = f"dfNew.`{column}`"

        return field_map_insert, field_map_update

    def update_control_table(self, source_path, source_name):
        control_table_entry = {
          "input_path": source_path,
          "input_name": source_name,
          "last_update": self.extract_current_timestamp(timestamp_format="unix"),
          "last_update_human": self.extract_current_timestamp(timestamp_format="%Y%m%d%H%M%S")
        }
        control_table_entry_spark = self.spark_session.createDataFrame([control_table_entry]).select(*control_table_entry.keys())
        
        control_table_entry_spark.write.mode("append").format("parquet").save(self.control_table)

    def set_catalog(self, catalog:str="catalog"):
        return catalog
    def set_schema(self, schema:str="schema"):
        return schema
    def set_table(self, table:str="table"):
        return table
    def extract_last_update(self, **kwargs):
        pass