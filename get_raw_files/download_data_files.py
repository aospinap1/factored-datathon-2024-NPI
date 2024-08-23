import os
import zipfile
import hashlib
import logging
import requests
from requests.exceptions import RequestException

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_zip_contents(destination_folder: str, zip_file_path: str):
    """
    Extract all contents from a ZIP file to a specified folder.

    Args:
        destination_folder (str): The folder where the contents will be extracted.
        zip_file_path (str): The path to the ZIP file to be extracted.

    Raises:
        zipfile.BadZipFile: If the provided file is not a valid ZIP file.
    """
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(destination_folder)
        logging.info(f"Successfully extracted {zip_file_path} to {destination_folder}")
    except zipfile.BadZipFile:
        logging.warning(f"{zip_file_path} is not a valid ZIP file and will be skipped.")
    except (IOError, OSError) as e:
        logging.error(f"Failed to extract {zip_file_path}: {e}")
        raise

def download_zip_file(download_link: str, folder_path: str, zip_file_name: str, expected_md5: str = None):
    """
    Downloads a ZIP file from a specified link and returns its path and MD5 checksum.

    Args:
        download_link (str): The URL of the ZIP file to be downloaded.
        folder_path (str): The local directory where the ZIP file will be stored.
        zip_file_name (str): The name to save the downloaded ZIP file as.
        expected_md5 (str, optional): The expected MD5 checksum to validate the file.

    Returns:
        tuple: A tuple containing the MD5 checksum of the downloaded file and the path to the ZIP file.

    Raises:
        RequestException: If there is an issue with the network request.
        IOError: If there is an issue with file operations.
    """
    try:
        # Send a GET request to download the ZIP file
        response = requests.get(download_link, stream=True, timeout=10)
        response.raise_for_status()

        zip_file_path = os.path.join(folder_path, zip_file_name)

        # Ensure the directory exists
        os.makedirs(folder_path, exist_ok=True)

        # Save the ZIP file to the specified path
        with open(zip_file_path, "wb") as file:
            file.write(response.content)
        logging.info(f"Successfully downloaded {zip_file_name} to {zip_file_path}")

        # Compute the MD5 checksum of the downloaded file
        md5 = hashlib.md5(open(zip_file_path, "rb").read()).hexdigest()

        return md5, zip_file_path

    except RequestException as e:
        logging.error(f"Error downloading the file from {download_link}: {e}")
        raise
    except (IOError, OSError) as e:
        logging.error(f"File operation failed: {e}")
        raise
