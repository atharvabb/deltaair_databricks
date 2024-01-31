# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS deltadb;

# COMMAND ----------

def create_table(table_name,schema,table_location):
    spark.sql(f"""CREATE EXTERNAL TABLE IF NOT EXISTS deltadb.{table_name}(
    {schema}
    ) LOCATION '{table_location}'
              """)

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/cleaned_zone/api")

