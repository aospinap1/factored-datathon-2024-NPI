import os
import boto3
import tempfile
import pandas as pd
from dotenv import load_dotenv
from botocore.exceptions import ClientError

def initialize_s3(region_name: str = "us-east-1"):
    """
    Initializes the S3 resource using credentials stored in environment variables.

    Parameters:
    region_name (str): The AWS region where the S3 bucket is located. Default is 'us-east-1'.

    Returns:
    s3: A Boto3 S3 resource object.
    """
    load_dotenv()
    try:
        s3 = boto3.resource(
            service_name='s3',
            region_name=region_name,
            aws_access_key_id=os.getenv("aws_access_key_id"),
            aws_secret_access_key=os.getenv("aws_secret_access_key"),
        )
        return s3
    except Exception as e:
        print(f"Error initializing S3 resource: {e}")
        raise

def upload_to_s3(s3, bucket_name: str, file_path: str, key: str):
    """
    Uploads a file to an S3 bucket and removes the local file after successful upload.

    Parameters:
    s3: The initialized S3 resource object.
    bucket_name (str): The name of the S3 bucket where the file will be uploaded.
    file_path (str): The local path of the file to be uploaded.
    key (str): The S3 key (path) where the file will be stored in the bucket.

    Raises:
    Exception: If there is an error during the upload process.
    """
    try:
        s3.Bucket(bucket_name).upload_file(Filename=file_path, Key=key)
        print(f"File '{key}' successfully uploaded to '{bucket_name}/{key}'.")
        os.remove(file_path)
    except ClientError as e:
        print(f"ClientError during file upload: {e}")
        raise
    except Exception as e:
        print(f"Error uploading file '{key}' to S3: {e}")
        raise

def load_existing_parquet(s3, bucket_name: str, file_name: str) -> pd.DataFrame:
    """
    Loads a Parquet file from an S3 bucket if it exists.

    Parameters:
    s3: The initialized S3 resource object.
    bucket_name (str): The name of the S3 bucket.
    file_name (str): The name of the file to load.

    Returns:
    pd.DataFrame: The DataFrame loaded from the Parquet file.
                 Returns None if the file does not exist.

    Raises:
    ClientError: If there is an issue accessing the S3 bucket or file.
    """
    try:
        obj = s3.Bucket(bucket_name).Object(file_name).get()
        # Crear un archivo temporal para escribir el contenido
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
            temp_file.write(obj['Body'].read())
            temp_file.flush()  # Asegura que todos los datos se escriban al disco

            # Cargar el archivo Parquet desde el archivo temporal
            df = pd.read_parquet(temp_file.name)
        return df
    except Exception as e:
        print(f"Error al cargar el archivo Parquet: {e}")
        raise
