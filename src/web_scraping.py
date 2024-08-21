import re
import utils
import requests
import pandas as pd
from bs4 import BeautifulSoup

def extract_file_info(element, date=None, file_status=0):
    """
    Extracts file information from a given HTML element.

    Parameters:
    element: The HTML element containing file information.
    date (str): The date associated with the file, if available.
    file_status (int): The status of the file download (0: not downloaded, 1: downloaded).

    Returns:
    list: A list containing the file's link, size, MD5 checksum, date, and status.
    """
    try:
        link = element["href"]
        info = element.next_sibling.split(" ")
        size = info[1][1:-3]
        md5 = info[-1][:-3]
        if date is None:
            date = link.split(".")[0]
        return [link, size, md5, date, file_status]
    except (IndexError, AttributeError) as e:
        print(f"Error processing element {element}: {e}")
        return None

def get_files_up_to_yesterday(url: str, start_date: str) -> pd.DataFrame:
    """
    Retrieves file information from the specified URL for all days starting from the 
    given start date up to yesterday. Parses the webpage to extract file links and metadata.

    Parameters:
    url (str): The URL of the webpage to scrape.
    start_date (str): The start date in the format 'YYYYMMDD' to begin scraping.

    Returns:
    pd.DataFrame: A DataFrame containing file information with columns:
        - link: The URL link to the file.
        - size: The size of the file.
        - MD5: The MD5 checksum of the file.
        - date: The date associated with the file.
        - status: Initial status of the file (0: not downloaded, 1: downloaded).
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        soup = BeautifulSoup(response.text, 'html.parser')
        num_days = utils.get_num_days(start_date)

        links_list = soup.find_all("a")[2:2 + (num_days - 1) * 2]

        lst = [extract_file_info(element) for element in links_list if extract_file_info(element) is not None]

        df = pd.DataFrame(lst, columns=["link", "size", "MD5", "date", "status"])
        return df
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return pd.DataFrame(columns=["link", "size", "MD5", "date", "status"])

def get_files_range_date(url: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Retrieves file information from the specified URL for the given date range.
    Parses the webpage to extract file links and metadata.

    Parameters:
    url (str): The URL of the webpage to scrape.
    start_date (str): The start date in the format 'YYYYMMDD'.
    end_date (str): The end date in the format 'YYYYMMDD'.

    Returns:
    pd.DataFrame: A DataFrame containing file information with columns:
        - link: The URL link to the file.
        - size: The size of the file.
        - MD5: The MD5 checksum of the file.
        - date: The date associated with the file.
        - status: Initial status of the file (0: not downloaded, 1: downloaded).
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        soup = BeautifulSoup(response.text, 'html.parser')
        range_dates = utils.generate_date_range(start_date, end_date)

        lst = []
        for date in range_dates:
            links_list = soup.find_all(text=re.compile(date), href=True)
            for element in links_list:
                file_info = extract_file_info(element, date)
                if file_info:
                    lst.append(file_info)

        df = pd.DataFrame(lst, columns=["link", "size", "MD5", "date", "status"])
        return df
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return pd.DataFrame(columns=["link", "size", "MD5", "date", "status"])
