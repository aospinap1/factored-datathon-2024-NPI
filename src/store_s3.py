import os
import boto3
from dotenv import load_dotenv
import process_data
import constants

def connect_store_s3(bucket_name: str, folder_name: str, file_path: str, file_name: str, region_name: str = "us-east-2"):
    """
    Connects to an Amazon S3 bucket and uploads a file.

    Args:
        bucket_name (str): The name of the S3 bucket.
        folder_name (str): The name of the S3 bucket folder.
        file_path (str): The local path to the file that needs to be uploaded.
        file_name (str): The desired name of the file in the S3 bucket.
        region_name (str, optional): The AWS region where the S3 bucket is located. Defaults to "us-east-2".

    Raises:
        Exception: If there is an issue with the S3 connection or file upload.
    """
    try:
        # Load AWS credentials from environment variables
        load_dotenv()

        # Initialize the S3 resource with credentials
        s3 = boto3.resource(
            service_name="s3",
            region_name=region_name,
            aws_access_key_id=os.getenv("aws_access_key_id"),
            aws_secret_access_key=os.getenv("aws_secret_access_key")
        )

        # Upload the file to the specified S3 bucket
        s3.Bucket(bucket_name).upload_file(Filename=file_path, Key=folder_name+"/"+file_name)
        print(f"File '{file_name}' successfully uploaded to '{bucket_name}'/'{folder_name}'.")

        os.remove(file_path)

    except Exception as e:
        print(f"Error uploading file '{file_name}' to S3: {e}")
        raise

def store_s3(bucket_name: str, column_name: str, topics: list):
    folder_name = "gkg"
    folder_path = os.path.join(constants.paths["data"], folder_name)
    for file in os.listdir(folder_path):
        file_path =os.path.join(folder_path, file)
        process_data.filter_topics(file_path, column_name, topics)
        connect_store_s3(bucket_name, folder_name, file_path, file)
