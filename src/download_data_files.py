import os
import zipfile
import requests
import constants
from utils import generate_date_range

def extract_zip_contents(destination_folder: str, zip_file_path: str):
    """
    Extract all contents from a ZIP file to a specified folder.

    Args:
        destination_folder (str): The folder where the contents will be extracted.
        zip_file_path (str): The path to the ZIP file to be extracted.
    """
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(destination_folder)
    except zipfile.BadZipFile:
        print(f"Warning: {zip_file_path} is not a valid ZIP file and will be skipped.")

def download_and_extract_zip_files(base_url: str, dates: list[str], data_type: str):
    """
    Download and extract ZIP files from a given URL based on the provided dates.

    Args:
        base_url (str): The base URL from which to download the ZIP files.
        dates (list[str]): A list of dates in 'YYYYMMDD' format.
        data_type (str): The type of data ('gkg' or 'events') to be downloaded.

    Raises:
        ValueError: If the data_type is not 'gkg' or 'events'.
    """
    if data_type not in {"gkg", "events"}:
        raise ValueError("data_type must be either 'gkg' or 'events'.")

    for date in dates:
        zip_file_name = f"{date}.zip"
        if data_type == "gkg":
            download_link = f"{base_url}/{data_type}/{date}.gkg.csv.zip"
        if data_type == "events":
            download_link = f"{base_url}/{data_type}/{date}.export.CSV.zip"
        response = requests.get(download_link, stream=True, timeout=10)
        folder_path = os.path.join(constants.paths["data"], data_type)
        zip_file_path = os.path.join(folder_path, zip_file_name)
        # Ensure the directory exists
        os.makedirs(folder_path, exist_ok=True)

        # Download the ZIP file
        with open(zip_file_path, "wb") as file:
            file.write(response.content)

        # Extract the contents of the ZIP file
        extract_zip_contents(folder_path, zip_file_path)

        # Remove the ZIP file after extraction
        os.remove(zip_file_path)

def download_data_files(base_url: str, start_date: str, end_date: str):
    """
    Download and extract both 'gkg' and 'events' data files for a given date range.

    Args:
        base_url (str): The base URL from which to download the data files.
        start_date (str): The start date in 'YYYYMMDD' format.
        end_date (str): The end date in 'YYYYMMDD' format.
    """
    dates = generate_date_range(start_date,end_date)
    # Download and extract GKG files
    download_and_extract_zip_files(base_url,dates,"gkg")
    # Download and extract events files
    download_and_extract_zip_files(base_url, dates, "events")
