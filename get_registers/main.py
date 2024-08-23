import os
import json
import datetime
import pandas as pd
import get_raw_files.constants as constants
import registers
import store_s3
import utils

def main():
    """
    Main function that handles the retrieval, updating, and saving of file registers.

    The function follows these steps:
    1. Loads parameters from a JSON configuration file.
    2. Initializes an S3 resource using stored credentials.
    3. Determines the date range for which data should be scraped.
    4. Attempts to load existing data from an S3 bucket.
    5. If no existing data is found, scrapes the data from the specified URL and saves it to S3.
    6. If existing data is found, updates the data with any missing dates and saves it back to S3.
    """
    # Load parameters from the JSON file
    parameters_file_path = os.path.join(constants.paths["config"], "parameters.json")
    with open(parameters_file_path, 'r', encoding="UTF-8") as file:
        params = json.load(file)

    print("Starting the file registers retrieving process...")

    # Initialize the S3 resource
    s3 = store_s3.initialize_s3()
    bucket_name = params["s3"]["bucket_name"]
    file_name = "registers.parquet"
    url = params["scraping"]["url"]
    start_date = params["scraping"]["start_date"]
    
    # Calculate the end date as yesterday's date
    edate = datetime.datetime.today() - datetime.timedelta(days=1)
    end_date = datetime.datetime.strftime(edate, "%Y%m%d")

    # Generate the complete range of dates
    range_dates = utils.generate_date_range(start_date, end_date)

    # Load existing data if available
    df = store_s3.load_existing_parquet(s3, bucket_name, file_name)

    if df is None:
        # If no data exists, scrape and save the initial data
        df = pd.DataFrame(
            registers.fetch_files_by_date_range(url, range_dates),
            columns=["LINK", "SIZE", "MD5", "DATE", "STATUS"]
        )
        file_path = os.path.join(constants.paths["data"], file_name)
        df.to_parquet(file_path)
        store_s3.upload_to_s3(s3, bucket_name, file_path, file_name)
    else:
        # Otherwise, update the existing data with missing dates
        registers.update_and_save_data(s3, df, url, range_dates, bucket_name, file_name)

if __name__ == "__main__":
    main()
