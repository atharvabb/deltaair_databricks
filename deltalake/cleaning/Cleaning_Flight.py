# Databricks notebook source
# MAGIC %run /deltalake/utilities/Create_Tables_in_deltadb

# COMMAND ----------

#defining  parameters
#flight_schema = "airport_code string,city string,country string,name string"
flight_file_format="csv"
flight_raw_path="dbfs:/mnt/raw_zone/flights-data/"
flight_cleaned_path="/dbfs/mnt/cleaned_zone/flight"
flight_schema="Year int,Month int,DayofMonth int,DayOfWeek int,DepTime int,CRSDepTime int,ArrTime int,CRSArrTime int,UniqueCarrier string,FlightNum int,TailNum string,ActualElapsedTime int,CRSElapsedTime int,AirTime int,ArrDelay int,DepDelay int,Origin string,Dest string,Distance int,TaxiIn int,TaxiOut int,Cancelled int,CancellationCode string,Diverted int,CarrierDelay int,WeatherDelay int,NASDelay int,SecurityDelay int,LateAircraftDelay int"



# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import *
current_date = datetime.now().strftime('%Y-%m-%d')

# COMMAND ----------

flight_base_df = spark.read.format(flight_file_format).option("header",True).schema(flight_schema)\
                .csv(flight_raw_path).filter(col("dw_load_date")==lit(current_date))

# COMMAND ----------

#CREAting tables
table_name="flight"
schema_list=flight_base_df.dtypes
print(schema_list)
schema=""
for col,dtype in schema_list:
    schema+=col+" "+dtype+","
create_table(table_name,schema[:-1],flight_cleaned_path)

# COMMAND ----------

result_df = flight_base_df.drop("CRSDepTime","CRSArrTime")

# COMMAND ----------

result_df.write.mode(saveMode="overwrite").saveAsTable(f"deltadb.{table_name}")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from deltadb.flight
