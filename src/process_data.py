import pandas as pd

def filter_topics(file_path: str, column_name: str, topics: list):
    """
    Filters rows in a CSV file based on the presence of specific topics in a given column.

    Args:
        file_path (str): The path to the CSV file to be filtered.
        column_name (str): The name of the column to search for topics.
        topics (list): A list of topics (strings) to filter the rows by.

    Raises:
        FileNotFoundError: If the file at file_path does not exist.
        KeyError: If the specified column_name does not exist in the CSV file.
        Exception: For any other issues during the filtering or saving process.
    """
    try:
        # Load the CSV file into a DataFrame
        df = pd.read_csv(file_path, sep="\t")

        # Check if the specified column exists
        if column_name not in df.columns:
            raise KeyError(f"Column '{column_name}' does not exist in the file.")

        # Filter the DataFrame based on the presence of any of the topics in the specified column
        df = df[df[column_name].str.contains("|".join(topics), na=False)]

        # Save the filtered DataFrame back to the same CSV file
        df.to_csv(file_path, index=False)

    except FileNotFoundError:
        print(f"Error: The file at '{file_path}' was not found.")
        raise
    except KeyError as e:
        print(f"Error: {e}")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise
