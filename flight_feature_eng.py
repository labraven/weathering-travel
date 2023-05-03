from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("feature_eng").getOrCreate()


raw_path = "/Users/ryan/data/raw/flights.parquet"

df = spark.read.parquet(raw_path)
raw_rows = df.count()
df.printSchema()
print(raw_rows)

flights_columns = ["year",
                 "month",
                 "day_of_month",
                 "day_of_week",
                 "dot_id_reporting_airline",
                 "origin_airport_id",
                 "dest_airport_id",
                 "crs_dep_time",
                 "dep_delay",
                 "dep_del15",
                 "crs_arr_time",
                 "arr_delay",
                 "arr_del15",
                 "cancelled"]


df = df.select(flights_columns)
df.show(4)