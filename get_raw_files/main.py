import os
import json
import constants
import store_s3
import download_data_files
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    # Load parameters from the JSON file
    parameters_file_path = os.path.join(constants.paths["config"], "parameters.json")
    with open(parameters_file_path, 'r', encoding="UTF-8") as file:
        params = json.load(file)
    
    s3 = store_s3.initialize_s3()
    bucket_name = params["s3"]["bucket_name"]
    file_name = "registers.parquet"

    # Load existing DataFrame from S3
    df = store_s3.load_existing_parquet(s3, bucket_name, file_name)
    if df is None or df.empty:
        logging.warning(f"No existing data found in {file_name}.")
        return

    # Filter rows with STATUS 0 (not downloaded)
    df_to_download = df[df["STATUS"] == 0]
    if df_to_download.empty:
        logging.info("All files have already been downloaded.")
        return

    # Iterate over rows to download, extract, and upload files
    for _, row in df_to_download.iterrows():
        zip_file_name = os.path.basename(row["LINK"])
        try:
            # Download and extract the ZIP file
            md5, zip_file_path = download_data_files.download_zip_file(
                download_link=row["LINK"],
                folder_path=constants.paths["data"],
                zip_file_name=zip_file_name,
                expected_md5=row["MD5"]
            )

            if md5 != row["MD5"]:
                logging.warning(f"MD5 checksum does not match for {zip_file_name}. Skipping file.")
                os.remove(zip_file_path)
                continue

            download_data_files.extract_zip_contents(constants.paths["data"], zip_file_path)

            # Remove the ZIP file after extraction
            os.remove(zip_file_path)

            # Upload the extracted file to S3
            extracted_file_name = zip_file_name.replace(".zip", "")
            extracted_file_path = os.path.join(constants.paths["data"], extracted_file_name)
            store_s3.upload_to_s3(s3, bucket_name, extracted_file_path, extracted_file_name)

            # Update the DataFrame to mark the file as downloaded
            df.loc[df["LINK"] == row["LINK"], "STATUS"] = 1
            logging.info(f"File {extracted_file_name} processed and uploaded successfully.")

        except Exception as e:
            logging.error(f"Failed to process {zip_file_name}: {e}")

    # Optionally save the updated DataFrame back to S3
    updated_file_path = os.path.join(constants.paths["data"], file_name)
    df.to_parquet(updated_file_path)
    store_s3.upload_to_s3(s3, bucket_name, updated_file_path, file_name)
    logging.info(f"Updated DataFrame saved and uploaded to {bucket_name}/{file_name}.")

if __name__ == "__main__":
    main()
