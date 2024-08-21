import os
import boto3
import process_data
import constants
from dotenv import load_dotenv

def connect_store_s3(bucket_name: str, folder_name: str, file_path: str, file_name: str, region_name: str = "us-east-2"):
    """
    Connects to an Amazon S3 bucket and uploads a file.

    Args:
        bucket_name (str): The name of the S3 bucket.
        folder_name (str): The name of the S3 bucket folder.
        file_path (str): The local path to the file that needs to be uploaded.
        file_name (str): The desired name of the file in the S3 bucket.
        region_name (str, optional): The AWS region where the S3 bucket is located.
        Defaults to "us-east-2".

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

def store_s3(bucket_name: str, column_name: str, topics: list, folder_name: str = "gkg"):
    """
    Filters CSV files in a specified folder for certain topics and uploads them to an S3 bucket.

    Parameters:
    bucket_name (str): The name of the S3 bucket where the files will be stored.
    column_name (str): The name of the column in the CSV files to be filtered.
    topics (list): A list of topics to filter by in the specified column.
    folder_name (str): The name of the folder containing the CSV files to process. Default is 'gkg'.

    Returns:
    None
    """
    folder_path = os.path.join(constants.paths["data"], folder_name)
    
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"The folder {folder_path} does not exist.")
    
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            file_path = os.path.join(folder_path, file_name)
            
            try:
                # Filter the CSV files based on the specified topics
                process_data.filter_topics(file_path, column_name, topics)
                
                # Upload the filtered file to the specified S3 bucket
                connect_store_s3(bucket_name, folder_name, file_path, file_name)
            
            except Exception as e:
                print(f"An error occurred while processing {file_name}: {e}")
