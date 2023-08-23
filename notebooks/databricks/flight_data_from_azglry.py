# Databricks notebook source
# MAGIC %md
# MAGIC Manual Steps:
# MAGIC 1. Create Azure Storage Account
# MAGIC 1. Create "azgallery" directory
# MAGIC 1. Upload csv file via Azure Storage Explorer
# MAGIC 1. renamed csv files to `snake_case`
# MAGIC
# MAGIC
# MAGIC TODO Need to store key in Databricks Secrets
# MAGIC

# COMMAND ----------

storage_account_name = "cgaistoragegen2"
storage_account_access_key = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-08-23T00:43:36Z&st=2023-08-22T16:43:36Z&spr=https&sig=NPtJy1qwrT79M%2FPGjJLC57bd%2BpB0T7aqrfrqmxDv8EY%3D"
container_name = "cgaibloblweatheringtravel"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type." + storage_account_name + ".dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type." + storage_account_name + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token." + storage_account_name + ".dfs.core.windows.net", storage_account_access_key)

# COMMAND ----------

afbss_address = "abfss://" + container_name + "@" + storage_account_name + ".dfs.core.windows.net"

# COMMAND ----------

flight_data_file_path = "/azgallery/flight_delays_data.csv"
file_location = afbss_address + flight_data_file_path
file_type = "csv"

flight_df = spark.read.format(file_type).option("inferSchema", "true").option("header", "true").load(file_location)
flight_df.display()

# COMMAND ----------

weather_data_file_path = "/azgallery/weather_dataset.csv"
file_location = afbss_address + weather_data_file_path
file_type = "csv"
weather_df = spark.read.format(file_type).option("inferSchema", "true").option("header", "true").load(file_location)
weather_df.display()

# COMMAND ----------

airport_data_file_path = "/azgallery/airports.csv"
file_location = afbss_address + airport_data_file_path
file_type = "csv"

airport_df = spark.read.format(file_type).option("inferSchema", "true").option("header", "true").load(file_location)
airport_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC https://gallery.azure.ai/Experiment/Binary-Classification-Flight-delay-prediction-1
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flight Data
# MAGIC - Passenger flight on-time performance data taken from TranStats data collection from U.S. Department of Transportation.
# MAGIC - The dataset contains flight delay data for the period April-October 2013. Before uploading the data to Azure ML Studio, we pre-processed it as follows:
# MAGIC
# MAGIC 1. Filtered to include only the 70 busiest airports in the continental United States.
# MAGIC 1. For canceled flights, relabeled as delayed by more than 15 mins.
# MAGIC 1. Filtered out diverted flights.
# MAGIC 1. From the dataset, we selected the following 14 columns:
# MAGIC - Year
# MAGIC - Month
# MAGIC - DayofMonth
# MAGIC - DayOfWeek
# MAGIC - Carrier - Code assigned by IATA and commonly used to identify a carrier.
# MAGIC - OriginAirportID - An identification number assigned by US DOT to identify a unique airport (the flight's origin).
# MAGIC - DestAirportID - An identification number assigned by US DOT to identify a unique airport (the flight's destination).
# MAGIC - CRSDepTime - The CRS departure time in local time (hhmm)
# MAGIC - DepDelay - Difference in minutes between the scheduled and actual departure times. Early departures show negative numbers.
# MAGIC - DepDel15 - A Boolean value indicating whether the departure was delayed by 15 minutes or more (1=Departure was delayed)
# MAGIC - CRSArrTime - CRS arrival time in local time(hhmm)
# MAGIC - ArrDelay - Difference in minutes between the scheduled and actual arrival times. Early arrivals show negative numbers.
# MAGIC - ArrDel15 - A Boolean value indicating whether the arrival was delayed by 15 minutes or more (1=Arrival was delayed)
# MAGIC - Cancelled - A Boolean value indicating whether the arrivalflight was cancelled (1=Flight was cancelled)
# MAGIC
# MAGIC ## Weather Data
# MAGIC We also used a set of weather observations: Hourly land-based weather observations from NOAA.
# MAGIC
# MAGIC The weather data represents observations from airport weather stations, covering the same time period of April-October 2013.
# MAGIC
# MAGIC Processing:
# MAGIC 1. Weather station IDs were mapped to corresponding airport IDs.
# MAGIC 1. Weather stations not associated with the 70 busiest airports were filtered out.
# MAGIC 1. The Date column was split into separate columns: Year, Month and Day.
# MAGIC 1. From the weather data, the following 26 columns were selected: AirportID, Year, Month, Day, Time, TimeZone, SkyCondition, Visibility, WeatherType, DryBulbFarenheit, DryBulbCelsius, WetBulbFarenheit, WetBulbCelsius, DewPointFarenheit, DewPointCelsius, RelativeHumidity, WindSpeed, WindDirection, ValueForWindCharacter, StationPressure, PressureTendency, PressureChange, SeaLevelPressure, RecordType, HourlyPrecip, Altimeter
# MAGIC
# MAGIC ## Airport Codes Dataset
# MAGIC
# MAGIC The final dataset used in the experiment contains one row for each U.S. airport, including the airport ID number, airport name, the city, and state (columns: *airport_id*, city, state, name).
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flight Data Preprocessing
# MAGIC
# MAGIC First, we used the Project Columns module to exclude from the dataset columns that are possible target leakers: DepDelay, DepDel15, ArrDelay, Cancelled, Year.

# COMMAND ----------

exclude_cols = ["DepDelay", "DepDel15", "ArrDelay", "Cancelled", "Year"]
flight_df = flight_df.drop(*exclude_cols).withColumn("late", fn.col("ArrDel15").cast("boolean"))
flight_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC The columns Carrier, OriginAirportID, and DestAirportID represent categorical attributes. However, because they are integers, they are initially parsed as continuous numbers; therefore, we used the Metadata Editor module to convert them to categorical.

# COMMAND ----------

import pyspark.sql.functions as fn
flight_df = flight_df \
  .withColumn("dep_time_rounded", fn.round(fn.column("CRSDepTime"), -2)) \
  .withColumn("arr_time_rounded", fn.round(fn.column("CRSArrTime"), -2))
flight_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weather Data Preprocessing
# MAGIC
# MAGIC Columns that have a large proportion of missing values are excluded using the Project Columns module. These include all string-valued columns: ValueForWindCharacter, WetBulbFarenheit, WetBulbCelsius, PressureTendency, PressureChange, SeaLevelPressure, and StationPressure.

# COMMAND ----------

exclude_weather_cols = "ValueForWindCharacter", "WetBulbFarenheit", "WetBulbCelsius", "PressureTendency", "PressureChange", "SeaLevelPressure", "StationPressure"
weather_df = weather_df.drop(*exclude_weather_cols)
weather_df.display()

# COMMAND ----------

# TODO The time adjustments are horrible. This should be done with TimeDate functions
weather_df = weather_df.withColumn("adjusted_time", (100 * fn.col("TimeZone")) + (100 * fn.floor(fn.column("Time")/100)) )
weather_df.display()

# COMMAND ----------

join_condition = (flight_df["OriginAirportID"] == weather_df["AirportID"]) & \
  (flight_df["Month"] == weather_df["Month"]) & \
  (flight_df["DayOfMonth"] == weather_df["Day"])
df = flight_df.join(weather_df, join_condition).drop(weather_df["AirportID"]).drop(weather_df["Month"]).drop(weather_df["Day"])
df.display()

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS flight_delays")
df.write.mode("overwrite").saveAsTable("flight_delays")

# COMMAND ----------


