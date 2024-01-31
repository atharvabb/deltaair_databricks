# Databricks notebook source
# MAGIC %run /deltalake/utilities/Create_Tables_in_deltadb

# COMMAND ----------

#defining  parameters
plane_schema = "tailnum string,type string,manufacturer string,issue_date date,model string,status string,aircraft_type string,engine_type string,year int,dw_load_date date"
plane_file_format="csv"
plane_raw_path="dbfs:/mnt/raw_zone/plane-data/"
planes_cleaned_path="dbfs:/mnt/cleaned_zone/planes"

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import *
current_date = datetime.now().strftime('%Y-%m-%d')

# COMMAND ----------

plane_base_df = spark.read.format(plane_file_format).schema(plane_schema).option("header",True)\
                .csv(plane_raw_path).filter(col("dw_load_date")==lit(current_date))\
                .drop("dw_load_date")

# COMMAND ----------

plane_base_df.show()
#CREAting tables
table_name="planes"
schema_list=plane_base_df.dtypes
print(schema_list)
schema=""
for col,dtype in schema_list:
    schema+=col+" "+dtype+","
create_table(table_name,schema[:-1],planes_cleaned_path)

# COMMAND ----------

plane_base_df.createOrReplaceTempView("planes_new")
display(spark.sql("select * from planes_new"))

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO deltadb.planes t
# MAGIC    USING (SELECT DISTINCT * FROM planes_new) s
# MAGIC    ON t.tailnum=s.tailnum
# MAGIC    WHEN MATCHED THEN UPDATE SET *
# MAGIC    WHEN NOT MATCHED THEN INSERT *
# MAGIC    
# MAGIC
