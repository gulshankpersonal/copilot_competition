import os
from pyspark.sql import SparkSession

def read_files_into_dataframe(folder_location, file_extension):
    if not os.path.exists(folder_location):
        raise ValueError("Folder does not exist")

    files = [file for file in os.listdir(folder_location) if file.endswith(file_extension)]
    if not files:
        raise ValueError("No files with the provided extension found")

    spark = SparkSession.builder.getOrCreate()

    supported_extensions = [".csv", ".parquet", ".json"]  # Add more supported extensions if needed
    if file_extension not in supported_extensions:
        raise ValueError("Unsupported file extension")

    dataframes = []
    for file in files:
        file_path = os.path.join(folder_location, file)
        dataframe = spark.read.format(file_extension).load(file_path)
        dataframes.append(dataframe)

    combined_dataframe = dataframes[0].unionAll(dataframes[1:]) if len(dataframes) > 1 else dataframes[0]

    return combined_dataframe