from pyspark.sql import SparkSession

def read_files(location, file_extension):
    spark = SparkSession.builder.getOrCreate()
    
    if file_extension == 'csv':
        df = spark.read.csv(location, header=True, inferSchema=True)
    elif file_extension == 'json':
        df = spark.read.json(location)
    elif file_extension == 'parquet':
        df = spark.read.parquet(location)
    elif file_extension == 'avro':
        df = spark.read.format("avro").load(location)
    else:
        raise ValueError("Unsupported file extension")
    
    return df

    pnr_sample = read_files('/mnt/ppeedp/raw/competition/pnr_sample', 'parquet')
    distance_master = read_files('/mnt/stppeedp/ppeedp/CBI2/production/reference_zone/distance_master', 'parquet')
    location_master = read_files('/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/Location_master', 'parquet')
    backtrackexception_master = read_files('/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/BacktrackExceptionmaster', 'parquet')