BUCKET_NAME:str = "ifood"

LANDING_LAYER_BUCKET_NAME:str = "landing"
LANDING_BRONZE_BUCKET_NAME:str = "bronze"

def generate_landing_layer_path(output_path:str) -> str:
    landing_layer_path  = f"s3a://{BUCKET_NAME}/{LANDING_LAYER_BUCKET_NAME}/{output_path}"

    return landing_layer_path

def generate_bronze_layer_path(output_path:str) -> str:
    bronze_layer_path  = f"s3a://{BUCKET_NAME}/{LANDING_BRONZE_BUCKET_NAME}/{output_path}"

    return bronze_layer_path



FIXED_SCHEMA_INGESTION = [
    ("VendorID", "int"),
    ("tpep_pickup_datetime", "timestamp"),
    ("tpep_dropoff_datetime", "timestamp"),
    ("passenger_count", "int"),
    ("trip_distance", "double"),
    ("RatecodeID", "int"),
    ("store_and_fwd_flag", "string"),
    ("PULocationID", "int"),
    ("DOLocationID", "int"),
    ("payment_type", "int"),
    ("fare_amount", "double"),
    ("extra", "double"),
    ("mta_tax", "double"),
    ("tip_amount", "double"),
    ("tolls_amount", "double"),
    ("improvement_surcharge", "double"),
    ("total_amount", "double"),
    ("congestion_surcharge", "double"),
    ("Airport_fee", "double")
]