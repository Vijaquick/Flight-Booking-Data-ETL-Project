# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Flights").getOrCreate()
json_flight_data=spark.read.option("multiline", True).json("/FileStore/tables/fightdataset.json")
# display(json_flight_data)

# COMMAND ----------

# DBTITLE 1,Trasformation
from pyspark.sql.functions import *
# Step 2: Explode flights array
df2 = json_flight_data.withColumn('flights', explode('flights'))

# Step 3: Extract flight booking status
flight_booking_status = df2.select(
    col('booking_date'),
    col('booking_id'),
    col('flights.arrive'),
    col('flights.depart'),
    col('flights.flight_no'),
    col('flights.from_city'),
    col('flights.to_city'),
    col('status')
)

# Step 4: Extract customer details
customer_details = df2.select(col('customer.*'))

# display(customer_details)

# COMMAND ----------

# DBTITLE 1,Load
# Step 5: Write flight data to Parquet
flight_booking_status.write\
    .format("parquet")\
    .mode("overwrite")\
    .save("/mnt/tables/flight_data")

# Step 6: Write customer data to Parquet
customer_details.write\
    .format("parquet")\
    .mode("overwrite")\
    .save("/mnt/tables/customer_data")