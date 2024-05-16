import os
from pyspark.sql import SparkSession

spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")

def read_files_into_dataframe(folder_location, file_extension):
    if not os.path.exists(folder_location):
        raise ValueError(f"The folder '{folder_location}' does not exist.")

    spark = SparkSession.builder.getOrCreate()

    files = [file for file in os.listdir(folder_location) if file.endswith(file_extension)]
    if not files:
        raise ValueError(f"No files with the extension '{file_extension}' found in the folder '{folder_location}'.")

    dfs = []
    for file in files:
        file_path = os.path.join(folder_location, file)
        try:
            df = spark.read.format(file_extension).load(file_path)
            dfs.append(df)
        except Exception as e:
            raise ValueError(f"Error reading file '{file_path}': {str(e)}")

    if not dfs:
        raise ValueError(f"No valid files found with the extension '{file_extension}' in the folder '{folder_location}'.")

    combined_df = dfs[0].unionAll(dfs[1:]) if len(dfs) > 1 else dfs[0]

    return combined_df

# Call the function to create DataFrames for each table
pnr_sample = read_files_into_dataframe('/mnt/ppeedp/raw/competition/pnr_sample', 'parquet')
distance_master = read_files_into_dataframe('/mnt/stppeedp/ppeedp/CBI2/production/reference_zone/distance_master', 'parquet')
location_master = read_files_into_dataframe('/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/Location_master', 'parquet')
backtrackexception_master = read_files_into_dataframe('/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/BacktrackExceptionmaster', 'parquet')

