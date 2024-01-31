# Databricks notebook source
# MAGIC %run /deltalake/utilities/Create_Tables_in_deltadb

# COMMAND ----------

#defining  parameters
api_raw_path="dbfs:/mnt/raw_zone/api-data/"
api_cleaned_path="dbfs:/mnt/cleaned_zone/api"

# COMMAND ----------

from pyspark.sql.functions import explode,col,from_json
api_base_df = spark.read.format("json").option("inferSchema",True).option("path","dbfs:/mnt/raw_zone/api-data/").load().select(explode("response"),col("dw_load_date")).select("col.*","dw_load_date")

# COMMAND ----------

from pyspark.sql.functions import when,lit
trans_api_df = api_base_df.withColumn("iata_code",when(col("iata_code").isNull(),"NA").otherwise(col("iata_code"))).withColumn("icao_code",when(col("icao_code").isNull(),"NA").otherwise(col("icao_code")))

# COMMAND ----------

#CREAting tables
table_name="api"
schema_list=trans_api_df.dtypes
print(schema_list)
schema=""
for col,dtype in schema_list:
    schema+=col+" "+dtype+","
create_table(table_name,schema[:-1],api_cleaned_path)

# COMMAND ----------

trans_api_df.write.mode("overwrite").saveAsTable("deltadb.api")
