# Databricks notebook source
# MAGIC %run /deltalake/utilities/Create_Tables_in_deltadb

# COMMAND ----------

#defining  parameters
airport_schema = "airport_code string,city string,country string,name string"
airport_file_format="text"
airport_raw_path="dbfs:/mnt/raw_zone/airport-data/"
airport_cleaned_path="/dbfs/mnt/cleaned_zone/airport"


# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import *
current_date = datetime.now().strftime('%Y-%m-%d')

# COMMAND ----------

airport_base_df = spark.read.format(airport_file_format).schema("values string").option("header",True)\
                .text(airport_raw_path).filter(col("dw_load_date")==lit(current_date))

# COMMAND ----------

#CREAting tables
table_name="airport"
schema_list=airport_base_df.dtypes
print(schema_list)
schema=""
for col,dtype in schema_list:
    schema+=col+" "+dtype+","
create_table(table_name,schema[:-1],airport_cleaned_path)

# COMMAND ----------

from pyspark.sql.functions import split,col,regexp_replace
transformed_df = airport_base_df.select(split(col("values"),",")[0].alias("airport_code"),split(col("values"),",")[1].alias("city"),split(split(col("values"),",")[2],":")[0].alias("country"),\
  split(col("values"),":")[1].alias("name"))\
    .withColumn("airport_code",regexp_replace(col("airport_code"),'"',''))\
    .withColumn("city",regexp_replace(col("city"),'"',''))\
    .withColumn("name",regexp_replace(col("name"),'"',''))
file_headers = transformed_df.first()
result_df = transformed_df.filter(col("city") != lit(file_headers.city))

result_df.createOrReplaceTempView("airport_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO deltadb.airport t
# MAGIC    USING airport_new s
# MAGIC    ON t.airport_code=s.airport_code
# MAGIC    WHEN MATCHED THEN UPDATE SET *
# MAGIC    WHEN NOT MATCHED THEN INSERT *
# MAGIC    
# MAGIC
