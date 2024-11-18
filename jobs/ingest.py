import os
import requests
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def download_file(url, local_path):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Arquivo baixado: {local_path}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Erro ao baixar o arquivo {url}: {e}")
        return False

def upload_to_minio(local_path, bucket, object_name, minio_config):
    """Envia um arquivo local para um bucket no MinIO."""
    s3_client = boto3.client(
        's3',
        endpoint_url=minio_config['endpoint'],
        aws_access_key_id=minio_config['access_key'],
        aws_secret_access_key=minio_config['secret_key']
    )
    try:
        folder_path = "landing/yellow_taxi_files"
        object_path = f"{folder_path}/{object_name}"
        
        with open(local_path, 'rb') as f:
            s3_client.upload_fileobj(f, bucket, object_path)
        print(f"Arquivo {object_name} enviado para {bucket}/{object_path}.")
        
        os.remove(local_path)
        print(f"Arquivo local removido: {local_path}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print(f"Erro de credenciais do MinIO: {e}")
    except Exception as e:
        print(f"Erro ao enviar o arquivo {object_name} para o MinIO: {e}")

def process_files_for_months(year, months, minio_config):
    """Faz o download e envio de arquivos para os meses e anos especificados."""
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"

    for month in months:
        month_str = f"{month:02d}"
        file_name = f"yellow_tripdata_{year}-{month_str}.parquet"
        file_url = f"{base_url}{year}-{month_str}.parquet"

        print(f"Processando: {file_name}")
        local_path = f"/tmp/{file_name}"

        if download_file(file_url, local_path):
            upload_to_minio(local_path, minio_config['bucket'], file_name, minio_config)

minio_config = {
    "endpoint": "http://minio:9000",
    "bucket": "ifood",
    "access_key": "minio",
    "secret_key": "minio123"
}

year = 2023
months = [1 ,2, 3, 4, 5]

process_files_for_months(year, months, minio_config)
