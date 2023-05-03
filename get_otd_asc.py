from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("get_otd_asc").getOrCreate()

# local_asc = '/Users/ryan/data/flights/ontime.td.201302.asc'
# local_csv = '/Users/ryan/data/flights/ontime.td.201302.csv'

local_path = "/Users/ryan/data/flights/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_2013_1/"
local_csv_file = "On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2013_1.csv"
raw_path = "/Users/ryan/data/raw/flights.parquet"
df = spark.read.csv(local_path + local_csv_file, header=True, inferSchema=True)
rows = df.count()
print("There are {} rows in the flights dataset".format(rows))

df.printSchema()

df = df.withColumnRenamed("Year", "year") \
    .withColumnRenamed("Quarter", "quarter") \
    .withColumnRenamed("month", "month") \
    .withColumnRenamed("DayofMonth", "day_of_month") \
    .withColumnRenamed("DayOfWeek", "day_of_week") \
    .withColumnRenamed("FlightDate", "flight_date") \
    .withColumnRenamed("Reporting_Airline", "reporting_airline") \
    .withColumnRenamed("DOT_ID_Reporting_Airline", "dot_id_reporting_airline") \
    .withColumnRenamed("IATA_CODE_Reporting_Airline", "iata_code_reporting_airline") \
    .withColumnRenamed("Tail_Number", "tail_number") \
    .withColumnRenamed("Flight_Number_Reporting_Airline", "flight_number_reporting_airline") \
    .withColumnRenamed("OriginAirportID", "origin_airport_id") \
    .withColumnRenamed("OriginAirportSeqID", "origin_airport_seq_id") \
    .withColumnRenamed("OriginCityMarketID", "origin_city_market_id") \
    .withColumnRenamed("Origin", "origin") \
    .withColumnRenamed("OriginCityName", "origin_city_name") \
    .withColumnRenamed("OriginState", "origin_state") \
    .withColumnRenamed("OriginStateFips", "origin_state_fips") \
    .withColumnRenamed("OriginStateName", "origin_state_name") \
    .withColumnRenamed("OriginWac", "origin_wac") \
    .withColumnRenamed("DestAirportID", "dest_airport_id") \
    .withColumnRenamed("DestAirportSeqID", "dest_airport_seq_id") \
    .withColumnRenamed("DestCityMarketID", "dest_city_market_id") \
    .withColumnRenamed("Dest", "dest") \
    .withColumnRenamed("DestCityName", "dest_city_name") \
    .withColumnRenamed("DestState", "dest_state") \
    .withColumnRenamed("DestStateFips", "dest_state_fips") \
    .withColumnRenamed("DestStateName", "dest_state_name") \
    .withColumnRenamed("DestWac", "dest_wac") \
    .withColumnRenamed("CRSDepTime", "crs_dep_time") \
    .withColumnRenamed("DepTime", "dep_time") \
    .withColumnRenamed("DepDelay", "dep_delay") \
    .withColumnRenamed("DepDelayMinutes", "dep_delay_minutes") \
    .withColumnRenamed("DepDel15", "dep_del15") \
    .withColumnRenamed("DepartureDelayGroups", "departure_delay_groups") \
    .withColumnRenamed("DepTimeBlk", "dep_time_blk") \
    .withColumnRenamed("TaxiOut", "taxi_out") \
    .withColumnRenamed("WheelsOff", "wheels_off") \
    .withColumnRenamed("WheelsOn", "wheels_on") \
    .withColumnRenamed("TaxiIn", "taxi_in") \
    .withColumnRenamed("CRSArrTime", "crs_arr_time") \
    .withColumnRenamed("ArrTime", "arr_time") \
    .withColumnRenamed("ArrDelay", "arr_delay") \
    .withColumnRenamed("ArrDelayMinutes", "arr_delay_minutes") \
    .withColumnRenamed("ArrDel15", "arr_del15") \
    .withColumnRenamed("ArrivalDelayGroups", "arrival_delay_groups") \
    .withColumnRenamed("ArrTimeBlk", "arr_time_blk") \
    .withColumnRenamed("Cancelled", "cancelled") \
    .withColumnRenamed("CancellationCode", "cancellation_code") \
    .withColumnRenamed("Diverted", "diverted") \
    .withColumnRenamed("CRSElapsedTime", "crs_elapsed_time") \
    .withColumnRenamed("ActualElapsedTime", "actual_elapsed_time") \
    .withColumnRenamed("AirTime", "air_time") \
    .withColumnRenamed("Flights", "flights") \
    .withColumnRenamed("Distance", "distance") \
    .withColumnRenamed("DistanceGroup", "distance_group") \
    .withColumnRenamed("CarrierDelay", "carrier_delay") \
    .withColumnRenamed("WeatherDelay", "weather_delay") \
    .withColumnRenamed("NASDelay", "nas_delay") \
    .withColumnRenamed("SecurityDelay", "security_delay") \
    .withColumnRenamed("LateAircraftDelay", "late_aircraft_delay") \
    .withColumnRenamed("FirstDepTime", "first_dep_time") \
    .withColumnRenamed("TotalAddGTime", "total_add_g_time") \
    .withColumnRenamed("LongestAddGTime", "longest_add_g_time") \
    .withColumnRenamed("DivAirportLandings", "div_airport_landings") \
    .withColumnRenamed("DivReachedDest", "div_reached_dest") \
    .withColumnRenamed("DivActualElapsedTime", "div_actual_elapsed_time") \
    .withColumnRenamed("DivArrDelay", "div_arr_delay") \
    .withColumnRenamed("DivDistance", "div_distance") \
    .withColumnRenamed("Div1Airport", "div1_airport") \
    .withColumnRenamed("Div1AirportID", "div1_airport_id") \
    .withColumnRenamed("Div1AirportSeqID", "div1_airport_seq_id") \
    .withColumnRenamed("Div1WheelsOn", "div1_wheels_on") \
    .withColumnRenamed("Div1TotalGTime", "div1_total_g_time") \
    .withColumnRenamed("Div1LongestGTime", "div1_longest_g_time") \
    .withColumnRenamed("Div1WheelsOff", "div1_wheels_off") \
    .withColumnRenamed("Div1TailNum", "div1_tail_num") \
    .withColumnRenamed("Div2Airport", "div2_airport") \
    .withColumnRenamed("Div2AirportID", "div2_airport_id") \
    .withColumnRenamed("Div2AirportSeqID", "div2_airport_seq_id") \
    .withColumnRenamed("Div2WheelsOn", "div2_wheels_on") \
    .withColumnRenamed("Div2TotalGTime", "div2_total_g_time") \
    .withColumnRenamed("Div2LongestGTime", "div2_longest_g_time") \
    .withColumnRenamed("Div2WheelsOff", "div2_wheels_off") \
    .withColumnRenamed("Div2TailNum", "div2_tail_num") \
    .withColumnRenamed("Div3Airport", "div3_airport") \
    .withColumnRenamed("Div3AirportID", "div3_airport_id") \
    .withColumnRenamed("Div3AirportSeqID", "div3_airport_seq_id") \
    .withColumnRenamed("Div3WheelsOn", "div3_wheels_on") \
    .withColumnRenamed("Div3TotalGTime", "div3_total_g_time") \
    .withColumnRenamed("Div3LongestGTime", "div3_longest_g_time") \
    .withColumnRenamed("Div3WheelsOff", "div3_wheels_off") \
    .withColumnRenamed("Div3TailNum", "div3_tail_num") \
    .withColumnRenamed("Div4Airport", "div4_airport") \
    .withColumnRenamed("Div4AirportID", "div4_airport_id") \
    .withColumnRenamed("Div4AirportSeqID", "div4_airport_seq_id") \
    .withColumnRenamed("Div4WheelsOn", "div4_wheels_on") \
    .withColumnRenamed("Div4TotalGTime", "div4_total_g_time") \
    .withColumnRenamed("Div4LongestGTime", "div4_longest_g_time") \
    .withColumnRenamed("Div4WheelsOff", "div4_wheels_off") \
    .withColumnRenamed("Div4TailNum", "div4_tail_num") \
    .withColumnRenamed("Div5Airport", "div5_airport") \
    .withColumnRenamed("Div5AirportID", "div5_airport_id") \
    .withColumnRenamed("Div5AirportSeqID", "div5_airport_seq_id") \
    .withColumnRenamed("Div5WheelsOn", "div5_wheels_on") \
    .withColumnRenamed("Div5TotalGTime", "div5_total_g_time") \
    .withColumnRenamed("Div5LongestGTime", "div5_longest_g_time") \
    .withColumnRenamed("Div5WheelsOff", "div5_wheels_off") \
    .withColumnRenamed("Div5TailNum", "div5_tail_num")


print(df.printSchema())
df.show(4)

df.write.parquet(raw_path,
                 partitionBy=["year", "month"],
                 mode="append")
