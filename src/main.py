import os
import json
import constants
import download_data_files

def main():
    """
    Main function to execute the entire workflow.

    This function reads parameters from a JSON file, initiates the scraping process 
    to download and store files, and provides status updates on the console.
    """

    # Load parameters from the JSON file
    parameters_file_path = os.path.join(constants.paths["config"], "parameters.json")
    with open(parameters_file_path, 'r', encoding="UTF-8") as file:
        params = json.load(file)

    print("Starting the file storage process...")

    # Execute the file scraping process using the parameters from the JSON file
    download_data_files.download_data_files(
        params["scraping"]["url"],
        params["scraping"]["start_date"],
        params["scraping"]["end_date"]
    )

    print("The files have been stored successfully.")

if __name__ == "__main__":
    main()
