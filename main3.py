import os
from pyspark.sql import SparkSession

def read_files_into_dataframe(folder_location, file_extension):
    if not os.path.exists(folder_location):
        raise ValueError("Folder does not exist")

    spark = SparkSession.builder.getOrCreate()

    files = [file for file in os.listdir(folder_location) if file.endswith(file_extension)]
    if not files:
        raise ValueError("No files with the provided extension found")

    dataframes = []
    for file in files:
        try:
            dataframe = spark.read.format(file_extension).load(os.path.join(folder_location, file))
            dataframes.append(dataframe)
        except Exception as e:
            raise ValueError(f"Error reading file {file}: {str(e)}")

    if not dataframes:
        raise ValueError("No files could be read")

    combined_dataframe = dataframes[0].unionAll(dataframes[1:])
    return combined_dataframe


