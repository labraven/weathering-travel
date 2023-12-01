import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as fn

spark = SparkSession.builder.master("local[1]").appName("convert_bts_asc").getOrCreate()

asc_directory = "/Users/ryan/Data/landing/bts/form_234/"
asc_file = "ontime.td.202308.asc"

df = spark.read.load(asc_directory + asc_file, format="csv", sep="|", header=False)
df.show(24, truncate=False)

bts_schema = StructType([
    StructField("marketing_carrier_code", StringType(), True, {"definition": "Two letter IATA code"}),
    StructField("marketing_carrier_flight_number", StringType(), True, {}),
    StructField("scheduled_operating_carrier_code", StringType(), True, {"definition": "Two letter IATA code"}),
    StructField("scheduled_operating_carrier_flight_number", StringType(), True, {}),
    StructField("actual_operating_carrier_code", StringType(), True, {"definition": "Two letter IATA code"}),
    StructField("actual_operating_carrier_flight_number", StringType(), True, {}),
    StructField("departure_airport_code", StringType(), True, {"definition": "Three letter Airport code"}),
    StructField("arrival_airport_code", StringType(), True, {"definition": "Three letter Airport code"}),
    StructField("data_of_flight_operation", IntegerType(), True, {"definition": "Format ccyymrndd"}),
    StructField("day_of_week_of_flight_operation", IntegerType(), True, {"definition": "Mon = 1 , Sun = 7"}),
    StructField("scheduled_departure_time_oag", IntegerType(), True,
                {"definition": "Scheduled departure time as shown in Official Airline Guide(OAG) Local time 24 hour clock"}),
    StructField("scheduled_departure_time_crs", IntegerType(), True,
                {"definition": "H Scheduled departure time as shown in CRS(selected by the Carrier) Local time 24 hour clock"}),
    StructField("gate_departure_time_actual", IntegerType(), True, {"definition": "I Gate departure time (actual) Local time 24 hour clock"}),
    StructField("scheduled_arrival_time_oag", IntegerType(), True,
                {"definition": "J Scheduled arrival time per OAG Local time 24 hour clock"}),
    StructField("scheduled_arrival_time_crs", IntegerType(), True, {"definition": "K Scheduled arrival time per CRS Local time 24 hour clock"}),
    StructField("gate_arrival_time_actual", IntegerType(), True, {"definition": "L Gate arrival time (actual) Local time 24 hour clock"}),
    StructField("difference_between_oag_and_crs_scheduled_departure_times", IntegerType(), True, {"definition": "M Difference between OAG and CRS scheduled departure times In minutes (2 hours= 120 min) G minus H"}),
    StructField("difference_between_oag_and_crs_scheduled_arrival_times", IntegerType(), True, {"definition": "N Difference between OAG and CRS scheduled arrival times In minutes - J minus K"}),
    StructField("scheduled_elapsed_time_per_crs", IntegerType(), True, {"definition": "O Scheduled elapsed time per CRS In minutes - K minus H"}),
    StructField("actual_gate_to_gate_time", IntegerType(), True, {"definition": "P Actual gate-to-gate time In minutes - L minus I"}),
    StructField("departure_delay_time_actual_minutes", IntegerType(), True, {"definition": "Q Departure delay time (actual minutes) In minutes - I minus H"}),
    StructField("arrival_delay_time_actual_minutes", IntegerType(), True, {"definition": "R Arrival delay time (actual minutes) In minutes - L minus K"}),
    StructField("elapsed_time_difference_actual_minutes", IntegerType(), True, {"definition": "S Elapsed time difference (actual minutes) In minutes - P minus O"}),
    StructField("wheels_off_time_actual", IntegerType(), True, {"definition": "T Wheels-off time (actual) Local time 24 hour clock"}),
    StructField("wheels_on_time_actual", IntegerType(), True, {"definition": "U Wheels-on time (actual) Local time 24 hour clock"}),
    StructField("aircraft_tail_number", StringType(), True, {}),
    StructField("cancellation_code", StringType(), True, {"definition": "Values are A,B,C, and D"}),
    StructField("minutes_late_for_delay_code_e_carrier_caused", IntegerType(), True, {"definition": "X Minutes late for Delay Code E - Carrier Caused In minutes"}),
    StructField("minutes_late_for_delay_code_f_weather", IntegerType(), True, {"definition": "Y Minutes late for Delay Code F - Weather In minutes"}),
    StructField("minutes_late_for_delay_code_g_national_aviation_system_nas", IntegerType(), True, {"definition": "Z Minutes late for Delay Code G - National Aviation System (NAS) In minutes"}),
    StructField("minutes_late_for_delay_code_h_security", IntegerType(), True, {"definition": "AA Minutes late for Delay Code H - Security In minutes"}),
    StructField("minutes_late_for_delay_code_i_late_arriving_flight_initial", IntegerType(), True, {"definition": "AB Minutes late for Delay Code I - Late Arriving Flight (Initial) In minutes"}),
    StructField("first_gate_departure_time_actual", IntegerType(), True, {"definition": "AC First gate departure time (actual) Local time 24 hour clock"}),
    StructField("total_ground_time_away_from_gate", IntegerType(), True, {"definition": "AD Total ground time away from gate In minutes"}),
    StructField("longest_ground_time_away_from_gate", IntegerType(), True, {"definition": "AE Longest ground time away from gate In minutes"}),
    StructField("number_of_landings_at_diverted_airports", IntegerType(), True, {"definition": "AF Number of landings at diverted airports 1 to 5 for diversions, 9 designates a fly return canceled flight"}),
    StructField("diverted_airport_code_1", StringType(), True, {"definition": "AG Diverted airport code 1 Three letter Airport code"}),
    StructField("wheels_on_time_at_diverted_airport", IntegerType(), True, {"definition": "AH Wheels-on time at diverted airport Local time 24 hour clock"}),
    StructField("total_ground_time_away_from_gate_at_diverted_airport", IntegerType(), True, {"definition": "AI Total ground time away from gate at diverted airport In minutes"}),
    StructField("longest_ground_time_away_from_gate_at_diverted_airport", IntegerType(), True, {"definition": "AJ Longest ground time away from gate at diverted airport In minutes"}),
    StructField("wheels_off_time_actual_at_diverted_airport", IntegerType(), True, {"definition": "AK Wheels-off time (actual) at diverted airport Local time 24 hour clock"}),
    StructField("aircraft_tail_number_2", StringType(), True, {}),
    StructField("diverted_airport_code_2", StringType(), True, {"definition": "AM Diverted airport code 2 Three letter Airport code"}),
    StructField("wheels_on_time_at_diverted_airport_2", IntegerType(), True, {"definition": "AN Wheels-on time at diverted airport Local time 24 hour clock"}),
    StructField("total_ground_time_away_from_gate_at_diverted_airport_2", IntegerType(), True, {"definition": "AO Total ground time away from gate at diverted airport In minutes"}),
    StructField("longest_ground_time_away_from_gate_at_diverted_airport_2", IntegerType(), True, {"definition": "AP Longest ground time away from gate at diverted airport In minutes"}),
    StructField("wheels_off_time_actual_at_diverted_airport_2", IntegerType(), True, {"definition": "AQ Wheels-off time (actual) at diverted airport Local time 24 hour clock"}),
    StructField("aircraft_tail_number_3", StringType(), True, {}),
    StructField("diverted_airport_code_3", StringType(), True, {"definition": "AS Diverted airport code 3 Three letter Airport code"}),
    StructField("wheels_on_time_at_diverted_airport_3", IntegerType(), True, {"definition": "AT Wheels-on time at diverted airport Local time 24 hour clock"}),
    StructField("total_ground_time_away_from_gate_at_diverted_airport_3", IntegerType(), True, {"definition": "AU Total ground time away from gate at diverted airport In minutes"}),
    StructField("longest_ground_time_away_from_gate_at_diverted_airport_3", IntegerType(), True, {"definition": "AV Longest ground time away from gate at diverted airport In minutes"}),
    StructField("wheels_off_time_actual_at_diverted_airport_3", IntegerType(), True, {"definition": "AW Wheels-off time (actual) at diverted airport Local time 24 hour clock"}),
    StructField("aircraft_tail_number_4", StringType(), True, {}),
    StructField("diverted_airport_code_4", StringType(), True, {"definition": "AY Diverted airport code 4 Three letter Airport code"}),
    StructField("wheels_on_time_at_diverted_airport_4", IntegerType(), True, {"definition": "AZ Wheels-on time at diverted airport Local time 24 hour clock"}),
    StructField("total_ground_time_away_from_gate_at_diverted_airport_4", IntegerType(), True, {"definition": "BA Total ground time away from gate at diverted airport In minutes"}),
    StructField("longest_ground_time_away_from_gate_at_diverted_airport_4", IntegerType(), True, {"definition": "BB Longest ground time away from gate at diverted airport In minutes"}),
    StructField("wheels_off_time_actual_at_diverted_airport_4", IntegerType(), True, {"definition": "BC Wheels-off time (actual) at diverted airport Local time 24 hour clock"}),
    StructField("aircraft_tail_number_5", StringType(), True, {}),
    StructField("diverted_airport_code_5", StringType(), True, {"definition": "BE Diverted airport code 5 Three letter Airport code"}),
    StructField("wheels_on_time_at_diverted_airport_5", IntegerType(), True, {"definition": "BF Wheels-on time at diverted airport Local time 24 hour clock"}),
    StructField("total_ground_time_away_from_gate_at_diverted_airport_5", IntegerType(), True, {"definition": "BG Total ground time away from gate at diverted airport In minutes"}),
    StructField("longest_ground_time_away_from_gate_at_diverted_airport_5", IntegerType(), True, {"definition": "BH Longest ground time away from gate at diverted airport In minutes"}),
    StructField("wheels_off_time_actual_at_diverted_airport_5", IntegerType(), True, {"definition": "BI Wheels-off time (actual) at diverted airport Local time 24 hour clock"}),
    StructField("aircraft_tail_number_6", StringType(), True, {}),
    StructField("form_type", StringType(), True, {"definition": "Form Type Usually FORM-1"}),
    StructField("duplicate", StringType(), True, {"definition": "Duplicate Usually N"})
])

df_bts = spark.read.load(asc_directory + asc_file, format="csv", sep="|", header=False, schema=bts_schema)
df_bts.show(24, truncate=False)