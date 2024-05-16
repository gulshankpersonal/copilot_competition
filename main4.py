import os
from pyspark.sql import SparkSession

def read_files_into_dataframe(folder_location, file_extension):
    """
    Read files with a specific extension from a folder into a PySpark DataFrame.

    Args:
        folder_location (str): The folder path where the files are located.
        file_extension (str): The extension of the files to be read.

    Returns:
        pyspark.sql.DataFrame: The combined DataFrame containing the data from all the files.

    Raises:
        ValueError: If the provided folder_location does not exist.
        ValueError: If no files with the provided extension are found.
        ValueError: If there is an issue reading the file.
        ValueError: If the provided file extension is unsupported.
    """
    # Check if the provided folder_location exists
    if not os.path.exists(folder_location):
        raise ValueError(f"The folder '{folder_location}' does not exist.")

    # Create a SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Search for files with the specified extension in the provided folder
    files = [file for file in os.listdir(folder_location) if file.endswith(file_extension)]

    # Check if any files with the provided extension are found
    if not files:
        raise ValueError(f"No files with the extension '{file_extension}' found in the folder '{folder_location}'.")

    # Read files based on their extension using PySpark methods
    try:
        dataframes = [spark.read.format(file_extension).load(os.path.join(folder_location, file)) for file in files]
    except Exception as e:
        raise ValueError(f"Error reading the file: {str(e)}")

    # Combine the DataFrames obtained from reading the files into a single DataFrame
    combined_dataframe = dataframes[0].unionAll(dataframes[1:]) if len(dataframes) > 1 else dataframes[0]

    return combined_dataframe


    # Call read_files_into_dataframe function to create dataframes for each table
    pnr_sample = read_files_into_dataframe('/mnt/ppeedp/raw/competition/pnr_sample', 'parquet')
    distance_master = read_files_into_dataframe('/mnt/stppeedp/ppeedp/CBI2/production/reference_zone/distance_master', 'parquet')
    location_master = read_files_into_dataframe('/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/Location_master', 'parquet')
    backtrackexception_master = read_files_into_dataframe('/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/BacktrackExceptionmaster', 'parquet')
