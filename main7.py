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

#columns
# id = PNR+PNRCreateDate+SegSequenceNumber_IssueAlnCode
# IssueAirlineCode = pnr_sample.OPT_ALN_CD
# true_od = OD based on circuit rule irrespective of airline
# online_od = OD based on circuit rule for online airlines where IssueAirlineCode = 'EY'
# online_od_itinerary = Airport codes separated by hyphen, itinerary for the OD
# PNR, PNRCreateDate, dep_date, dep_time, arrival_date, arrival_time as null
# online_od_distance 
# length_of_stay = Difference between arrival date and departure date of the segment
# journey_operating = Airport code followed by operating career code for the whole journey;  Example - MIA AI JFK EY AUH EY BOM
# point_of_commencement = First Airport code of the whole journey
# point_of_finish = Last Airport code of the whole journey
# true_od_distance = Distance between origin and destination for true OD
##########
