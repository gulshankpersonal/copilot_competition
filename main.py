from pyspark.sql import SparkSession
from pyspark.sql.functions import concat
from pyspark.sql.functions import lit
from pyspark.sql.functions import col, sin, cos,  sqrt, radians
from pyspark.sql.functions import when
from pyspark.sql.functions import sum, first, last, collect_list, concat_ws, concat, atan2
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.sql.types import DoubleType

#initialize spark session
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()


ppe_table_details = """
    'pnr_sample_df' located at '/mnt/ppeedp/raw/competition/pnr_sample' with the format 'delta'},
    'distance_master_df' located at '/mnt/stppeedp/ppeedp/CBI2/production/reference_zone/distance_master' with the format 'delta'},
    'location_master_df' located at '/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/Location_master' with the format 'delta'},
    'backtrackexception_master_df' located at '/mnt/stppeedp/ppeedp/raw/eag/ey/test_cbi_reference_data_loader/target_dir/BacktrackExceptionmaster' with the format 'delta'}
"""

local_table_datails = """    
        'pnr_sample_df' located at ‘part-00000-pnr-sample.snappy.parquet' with the format ‘parquet'.
        'distance_master_df' located at 'part-00000-distance-master.snappy.parquet' with the format ‘parquet'.
        'location_master_df' located at 'part-00000-location-master.snappy.parquet' with the format ‘parquet'.
        'backtrackexception_master_df' located at 'part-00000-backtrackexception-master.snappy.parquet' with the format ‘parquet’.
        """
write_mode = 'overwrite'

ppe_write_path = '/mnt/stppeedp/ppeedp/raw/eag/ey/eygulshank/result/'
local_write_path = 'final_df.parquet'

ppe_write_format = 'delta'
local_write_format = 'parquet'

context = 'ppe'

#create table_detail, write_path, write_format as variables and initialize them to none 
table_details = None
write_path = None
write_format = None
# set table_details, write_path and write_format based on the context
if context == 'ppe':
    table_details = ppe_table_details
    write_path = ppe_write_path
    write_format = ppe_write_format
elif context == 'local':
    table_details = local_table_datails
    write_path = local_write_path
    write_format = local_write_format
else:
    raise ValueError("Invalid context. Please choose 'ppe' or 'local'.")

# Define a Pyspark function named read_files_into_dataframe with two parameters: location (a string representing the location of the files) and format (a string representing the format of the files).
#     Assume spark session is already available.
#     Read the file as per the format from the given location and returns the dataframe.
    
#     If there are any issues reading the files, ensure the function raises a ValueError with an appropriate error message from inside the function.

def read_files_into_dataframe(location, format):
    try:
        df = spark.read.format(format).load(location)
        return df
    except Exception as e:
        raise ValueError(f"Error reading files from {location} with format {format}: {str(e)}")

# Create a function named convert_location_to_decimal that takes a single argument location.
#     The function should handle the conversion of location coordinates from degrees, minutes, and seconds to decimal format.
#     Inside the function, initialize a variable named direction to store the last character of the location argument.
#     Determine the sign based on the direction (N, S, E, W). If the direction is N or E, set the sign to 1, otherwise set it to -1.
#     Remove the direction character from the location string.
#     Split the location string by the period (.) character.
#     Extract the degree, minute, and second components from the split location string.
#     If there is no period in the location, set minute and second to 0. If there's one period, set second to 0.
#     Convert degree, minute, and second to integers.
#     Calculate the decimal value using the formula: sign * (degree + minute / 60 + second / 3600).
#     Handle any exceptions that may occur during the conversion process and return 0 in case of an error.
#     Create a user-defined function (UDF) named convert_location_to_decimal_udf that wraps the convert_location_to_decimal function.
#     The UDF should have a return type of FloatType().
def convert_location_to_decimal(location):
    try:
        direction = location[-1]
        sign = 1 if direction in ['N', 'E'] else -1
        location = location[:-1]

        # Split the location string by '.' and handle cases with two or more '.' characters
        components = location.split('.')
        degree = components[0]
        minute = components[1] if len(components) > 1 else '0'
        second = components[2] if len(components) > 2 else '0'

        return sign * (int(degree) + int(minute) / 60 + int(second) / 3600)
    except Exception as e:
        return None

convert_location_to_decimal_udf = udf(convert_location_to_decimal, FloatType())


#call the above function to create DataFrames for each of the tables in the table_details
pnr_sample_df = read_files_into_dataframe('part-00000-pnr-sample.snappy.parquet', 'parquet')
distance_master_df = read_files_into_dataframe('part-00000-distance-master.snappy.parquet', 'parquet')
location_master_df = read_files_into_dataframe('part-00000-location-master.snappy.parquet', 'parquet')
backtrackexception_master_df = read_files_into_dataframe('part-00000-backtrackexception-master.snappy.parquet', 'parquet')


# apply the UDF to the location column in the location_master DataFrame
location_master_df = location_master_df.withColumn('latitude', convert_location_to_decimal_udf('latitude'))
location_master_df = location_master_df.withColumn('longitude', convert_location_to_decimal_udf('longitude'))

# Rename columns in pnr_sample_df
pnr_sample_df = pnr_sample_df.withColumnRenamed("SEG_SEQ_NBR", "SegSequenceNumber") \
                             .withColumnRenamed("OPT_ALN_CD", "IssueAlnCode") \
                             .withColumnRenamed("ORIG_AP_CD", "Origin_Airport_Code") \
                             .withColumnRenamed("DESTN_AP_CD", "Destination_Airport_Code") \
                             .withColumnRenamed("fl_dt", "flight_date")


location_master_df = location_master_df.withColumnRenamed("ap_cd_val", "airport_code")
# apply the UDF to the location column in the location_master DataFrame
location_master = location_master_df.withColumn('latitude', convert_location_to_decimal_udf('latitude'))
location_master = location_master_df.withColumn('longitude', convert_location_to_decimal_udf('longitude'))

pnr_sample_df = pnr_sample_df.withColumn("pnr", lit(None)).withColumn("dep_date", lit(None)).withColumn("dep_time", lit(None)).withColumn("arrival_date", lit(None)).withColumn("arrival_time", lit(None)).withColumn("length_of_stay", lit(None)).withColumn("PNRCreateDate", lit(None))

# remove id column from distance_master dataframe
distance_master_df = distance_master_df.drop("id")

pnr_sample_df = pnr_sample_df.withColumn("id", concat("PNR", "PNRCreateDate", "SegSequenceNumber", "IssueAlnCode"))


# Join pnr_sample_df with distance_master_df
joined_df = pnr_sample_df.join(distance_master_df,
                              (pnr_sample_df["Origin_Airport_Code"] == distance_master_df["airport_origin_code"]) &
                              (pnr_sample_df["Destination_Airport_Code"] == distance_master_df["airport_destination_code"]),
                              "left")

# select all the columns of pnr_sample_df and distance_in_km and distance_in_miles columns from joined_df
result_df = joined_df.select(pnr_sample_df.columns + ["distance_in_km", "distance_in_miles"])

# do a left join with location_master_df on Origin_Airport_Code=airport_code and latitude renamed 
# to origin_latitude and longitude renamed to origin_longitude
result_df = result_df.join(location_master_df.withColumnRenamed("latitude", "origin_latitude").withColumnRenamed("longitude", "origin_longitude"),
                           result_df["Origin_Airport_Code"] == location_master_df["airport_code"],
                           "left")
#remove duplicate columns from the result_df
result_df = result_df.drop("airport_code")

# do a left join with location_master_df on Destination_Airport_Code=airport_code and 
# latitude renamed to destination_latitude and longitude renamed to destination_longitude  
result_df = result_df.join(location_master_df.withColumnRenamed("latitude", "destination_latitude").withColumnRenamed("longitude", "destination_longitude"),
                           result_df["Destination_Airport_Code"] == location_master_df["airport_code"],
                           "left")

# remove duplicate columns from the result_df
result_df = result_df.drop("airport_code")

# Calculate distance in km using haversine formula
result_df = result_df.withColumn("distance_in_km", 
                                 when(col("distance_in_km").isNull(),
                                      6371 * atan2(sqrt(sin(radians(col("destination_latitude") - col("origin_latitude")) / 2) ** 2 +
                                                        cos(radians(col("origin_latitude"))) * cos(radians(col("destination_latitude"))) *
                                                        sin(radians(col("destination_longitude") - col("origin_longitude")) / 2) ** 2),
                                                   sqrt(1 - sin(radians(col("destination_latitude") - col("origin_latitude")) / 2) ** 2 +
                                                        cos(radians(col("origin_latitude"))) * cos(radians(col("destination_latitude"))) *
                                                        sin(radians(col("destination_longitude") - col("origin_longitude")) / 2) ** 2))
                                     ).otherwise(col("distance_in_km")))

result_df = result_df.withColumn("distance_in_miles", 
                           when(col("distance_in_miles").isNull(),
                               3959 * atan2(sqrt(sin(radians(col("destination_latitude") - col("origin_latitude")) / 2) ** 2 +
                                             cos(radians(col("origin_latitude"))) * cos(radians(col("destination_latitude"))) *
                                             sin(radians(col("destination_longitude") - col("origin_longitude")) / 2) ** 2),
                                         sqrt(1 - sin(radians(col("destination_latitude") - col("origin_latitude")) / 2) ** 2 +
                                             cos(radians(col("origin_latitude"))) * cos(radians(col("destination_latitude"))) *
                                             sin(radians(col("destination_longitude") - col("origin_longitude")) / 2) ** 2))
                              ).otherwise(col("distance_in_miles"))
                              )

# Group by pnrhash and calculate sum of distance_in_km and store it in tdc dataframe
tdc = result_df.groupBy("PNRHash").agg(sum("distance_in_km").alias("total_distance_covered"))
# Group by pnrhash and calculate first of origin_airport_code and store it in ffd dataframe
ffd = result_df.groupBy("PNRHash").agg(first("Origin_Airport_Code").alias("first_origin_airport_code"))
# Group by pnrhash and calculate last of destination_airport_code and store it in lfd dataframe
lfd = result_df.groupBy("PNRHash").agg(last("Destination_Airport_Code").alias("last_destination_airport_code"))
# take pnrhash, origin_airport_code, destination_airport_code, IssueAlnCode 
# from result_df and store it in odframe dataframe
odframe = result_df.select("PNRHash", "Origin_Airport_Code", "Destination_Airport_Code", "IssueAlnCode")
# create a column origin_issue_airline in odf by concatenating origin_airport_code and IssueAlnCode with space
odframe = odframe.withColumn("origin_issue_airline", concat("Origin_Airport_Code", lit(" "), "IssueAlnCode"))
# Group by pnrhash and create od by collecting all the origin_issue_airline values  and 
# also create a column airport_codes_list by collecting all the origin_airport_code values in the list
odframe = odframe.groupBy("PNRHash").agg(collect_list(col("origin_issue_airline")).alias("od"), collect_list(col("Origin_Airport_Code")).alias("airport_codes_list"))
# modify the od by joining the values in the list with space and airport_codes_list with hyphen
odframe = odframe.withColumn("od", concat_ws(" ", "od")).withColumn("airport_codes_list", concat_ws("-", "airport_codes_list"))
# craete orig_dest_df by joining lfd and ffd on pnrhash and create a new column 
# orig_dest by concatenating first_origin_airport_code and last_destination_airport_code without space
orig_dest_df = ffd.join(lfd, "PNRHash").withColumn("orig_dest", concat("first_origin_airport_code", "last_destination_airport_code"))
# concatenate od column last_destination_airport_code using space and also concatenate airport_codes_list and last_destination_airport_code using hyphen
#odframe = odframe.withColumn("od", concat(col("od"), lit(" "), col("last_destination_airport_code"))).withColumn("airport_codes_list", concat_ws("-", col("airport_codes_list"), col("last_destination_airport_code")))
# create a new column orig_dest_exception in backtrackexception_master_df by concatenating origin_airport_code 
# and destination_airport_code without space
backtrackexception_master_df = backtrackexception_master_df.withColumn("orig_dest_exception", concat("orig", "dest"))
# left join orig_dest_df with backtrackexception_master_df on orig_dest=orig_dest_exception 
# and create a new column exception_flag such that when orig_dest_exception is not null then 'Y' else 'N' 
# store it in exception_df
exception_df = orig_dest_df.join(backtrackexception_master_df, orig_dest_df["orig_dest"] == backtrackexception_master_df["orig_dest_exception"], "left").withColumn("exception_flag", when(col("orig_dest_exception").isNotNull(), "Y").otherwise("N"))
# left join odframe with exception_df on pnrhash and store it in odf2 dataframe
odframe2 = odframe.join(exception_df, "PNRHash", "left")
# left join odframe2 with tdc on pnrhash and store it in odframe3 dataframe
odframe3 = odframe2.join(tdc, "PNRHash", "left")
# take odframe3 do a left join with location_master_df on first_origin_airport_code=airport_code 
# and latitude renamed to start_latitude and longitude renamed to start_longitude 
odframe3 = odframe3.join(location_master_df.withColumnRenamed("latitude", "start_latitude").withColumnRenamed("longitude", "start_longitude"),
                         odframe3["first_origin_airport_code"] == location_master_df["airport_code"],
                         "left")
# remove duplicate columns from odframe3
odframe3 = odframe3.drop("airport_code")
# take odframe3 do a left join with location_master_df on last_destination_airport_code=airport_code 
# and latitude renamed to finish_latitude and longitude renamed to finish_longitude
odframe3 = odframe3.join(location_master_df.withColumnRenamed("latitude", "finish_latitude").withColumnRenamed("longitude", "finish_longitude"),
                         odframe3["last_destination_airport_code"] == location_master_df["airport_code"],
                         "left")
# remove duplicate columns from odframe3
odframe3 = odframe3.drop("airport_code")
#join odframe3 with distance_master_df on first_origin_airport_code=airport_origin_code and 
# last_destination_airport_code=airport_destination_code and take disance_in_km as start_finish_direct_distance
odframe3 = odframe3.join(distance_master_df.withColumnRenamed("distance_in_km", "start_finish_direct_distance"),
                         (odframe3["first_origin_airport_code"] == distance_master_df["airport_origin_code"]) &
                         (odframe3["last_destination_airport_code"] == distance_master_df["airport_destination_code"]),
                         "left")
#remove duplicate columns from odframe3
odframe3 = odframe3.drop("airport_origin_code", "airport_destination_code")
#odframe3.show()
# for rows where start_finish_direct_distance is null, calculate the distance using haversine formula and 
# store it in start_finish_direct_distance
# use start_latitude, start_longitude, finish_latitude, finish_longitude for the calculation
odframe3 = odframe3.withColumn("start_finish_direct_distance",
                               when(col("start_finish_direct_distance").isNull(),
                                    6371 * atan2(sqrt(sin(radians(col("finish_latitude") - col("start_latitude")) / 2) ** 2 +
                                                      cos(radians(col("start_latitude"))) * cos(radians(col("finish_latitude"))) *
                                                      sin(radians(col("finish_longitude") - col("start_longitude")) / 2) ** 2),
                                                 sqrt(1 - sin(radians(col("finish_latitude") - col("start_latitude")) / 2) ** 2 +
                                                      cos(radians(col("start_latitude"))) * cos(radians(col("finish_latitude"))) *
                                                      sin(radians(col("finish_longitude") - col("start_longitude")) / 2) ** 2))
                                   ).otherwise(col("start_finish_direct_distance"))
                               )

# Create a column circuit_rule_distance_flag such that when start_finish_direct_distance*1.6 is 
# less than total_distance_covered then 'Y' else 'N'
odframe3 = odframe3.withColumn("circuit_rule_distance_flag", when(col("start_finish_direct_distance") * 1.6 < col("total_distance_covered"), "Y").otherwise("N"))
# concatenate last_destination_airport_code to od using space and also concatenate 
# last_destination_airport_code to airport_codes_list using hyphen
odframe3 = odframe3.withColumn("od", concat(col("od"), lit(" "), col("last_destination_airport_code"))).withColumn("airport_codes_list", concat_ws("-", col("airport_codes_list"), col("last_destination_airport_code")))
# create a column od_new such that when exception_flag is 'Y' then orig_dest else 
# (if circuit_rule_distance_flag is 'Y' then od orig_dest)
odframe3 = odframe3.withColumn("od_new", when(col("exception_flag") == "Y", col("orig_dest")).otherwise(when(col("circuit_rule_distance_flag") == "Y", col("od")).otherwise(col("orig_dest"))))
# create a column true_od_distance as start_finish_direct_distance if (od_new is orig_dest 
# and (exception_flag='Y' or circuit_rule_distance_flag = 'N' )else total_distance_covered
odframe3 = odframe3.withColumn("true_od_distance", when((col("od_new") == col("orig_dest")) & ((col("exception_flag") == "Y") | (col("circuit_rule_distance_flag") == "N")), col("start_finish_direct_distance")).otherwise(col("total_distance_covered")))
# # create a new column online_od_distance as true_od_distance if ' EY ' in od  else null
odframe3 = odframe3.withColumn("online_od_distance", when(col("od").contains(" EY "), col("true_od_distance")).otherwise(None))
# create a new column online_od if ' EY ' in od_new then od_new else null
odframe3 = odframe3.withColumn("online_od", when(col("od_new").contains(" EY "), col("od_new")).otherwise(None))
# rename airport_codes_list to online_od_itinerary, od to journey_operating, first_origin_airport_code 
# to point_of_commencement, last_destination_airport_code to point_of_finish, od_new to true_od
odframe3 = odframe3.withColumnRenamed("airport_codes_list", "online_od_itinerary").withColumnRenamed("od", "journey_operating").withColumnRenamed("first_origin_airport_code", "point_of_commencement").withColumnRenamed("last_destination_airport_code", "point_of_finish").withColumnRenamed("od_new", "true_od")
#odframe3.show()
# select id, PNR, PNRHash, PNRCreateDate, IssueAlnCode, SegSequenceNumber,  true_od, online_od, online_od_itinerary, 
# dep_date, dep_time, arrival_date, arrival_time, online_od_distance, length_of_stay, journey_operating,
# point_of_commencement, point_of_finish, true_od_distance from result_df left joined with odframe3 on pnrhash
final_df = result_df.join(odframe3, "PNRHash", "left")\
    .select("id", "PNR", "pnrhash", "PNRCreateDate", "IssueAlnCode", "true_od", "online_od", "online_od_itinerary", "dep_date", "dep_time", "arrival_date", "arrival_time", "online_od_distance", "length_of_stay", "journey_operating", "point_of_commencement", "point_of_finish", "true_od_distance")
#final_df.show()

# overwrite final df as parquet file into the write_path as per the write_format and write_mode 
final_df.write.format(write_format).mode(write_mode).save(write_path)

# read and display the result from the write_path using read_files_into_dataframe function and format as write_format into a dataframe
read_result_df = read_files_into_dataframe(write_path, write_format)
#store it in any temp view and display the result
read_result_df.createOrReplaceTempView("final_df")
spark.sql("SELECT * FROM final_df").show()











