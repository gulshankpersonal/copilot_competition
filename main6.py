from pyspark.sql import SparkSession

def read_files_into_dataframe(location, format):
    try:
        return spark.read.format(format).load(location)
    except Exception as e:
        raise ValueError(f"Error reading files from {location} with format {format}: {str(e)}")

# Create DataFrames for each table
pnr_sample = read_files_into_dataframe('/mnt/ppeedp/raw/competition/pnr_sample', 'delta')
distance_master = read_files_into_dataframe('/mnt/stppeedp/ppeedp/CBI2/production/reference_zone/distance_master', 'delta')
location_master = read_files_into_dataframe('/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/Location_master', 'delta')
backtrackexception_master = read_files_into_dataframe('/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/BacktrackExceptionmaster', 'delta')

