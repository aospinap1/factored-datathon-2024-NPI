import re
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
import constants
import store_s3

def extract_file_info(base_url: str, element, date: str, file_status=0) -> list:
    """
    Extracts file information from an HTML element.

    Parameters:
    base_url (str): The base URL where the file is located.
    element (Tag): The HTML element containing file information.
    date (str): The date associated with the file.
    file_status (int): Status of the file (0 if not downloaded, 1 if downloaded). Default is 0.

    Returns:
    list: A list containing the file's link, size, MD5 checksum, date, and status, or None if an error occurs.
    """
    try:
        link = base_url.replace("index.html", "") + element["href"]
        info = element.next_sibling.split(" ")
        size = info[1][1:-3]
        md5 = info[-1][:-3]
        return [link, size, md5, date, file_status]
    except (IndexError, AttributeError) as e:
        print(f"Error processing element {element}: {e}")
        return None

def fetch_files_by_date_range(url: str, date_range: list) -> list:
    """
    Retrieves file information from the specified URL for a given range of dates.

    Parameters:
    url (str): The URL of the webpage to scrape.
    date_range (list): A list of dates to search for in the webpage.

    Returns:
    list: A list of file information for the specified date range.
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an error for bad status codes
        soup = BeautifulSoup(response.text, 'html.parser')

        file_info_list = []
        for date in date_range:
            links_list = soup.find_all(text=re.compile(date), href=True)
            for element in links_list:
                file_info = extract_file_info(url, element, date)
                if file_info:
                    file_info_list.append(file_info)

        return file_info_list
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return []

def update_dataframe_with_new_files(df: pd.DataFrame, url: str, missing_dates: list) -> pd.DataFrame:
    """
    Updates the existing DataFrame with new file information for missing dates.

    Parameters:
    df (pd.DataFrame): The existing DataFrame with file information.
    url (str): The URL to fetch the new data from.
    missing_dates (list): A list of dates to check for missing data.

    Returns:
    pd.DataFrame: The updated DataFrame including the new file information.
    """
    if missing_dates:
        new_files_info = fetch_files_by_date_range(url, missing_dates)
        df_new = pd.DataFrame(new_files_info, columns=df.columns)
        df = pd.concat([df, df_new], ignore_index=True)
        df.sort_values(by="DATE", inplace=True)

    return df


def update_and_save_data(s3, df: pd.DataFrame, url: str, date_range: list, bucket_name: str, file_name: str):
    """
    Updates the DataFrame with missing dates, saves the updated data as a Parquet file, and uploads it to S3.

    Parameters:
    s3: The initialized S3 resource object.
    df (pd.DataFrame): The existing DataFrame with file information.
    url (str): The URL to fetch the new data from.
    date_range (list): A list of dates to check for missing data.
    bucket_name (str): The name of the S3 bucket to upload the file to.
    file_name (str): The name of the Parquet file to save.

    Returns:
    None
    """
    existing_dates = df["DATE"].to_list()
    missing_dates = list(set(date_range).difference(set(existing_dates)))

    df_updated = update_dataframe_with_new_files(df, url, missing_dates)
    file_path = os.path.join(constants.paths["data"], file_name)
    df_updated.to_parquet(file_path)
    store_s3.upload_to_s3(s3, bucket_name, file_path, file_name)
