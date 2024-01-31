# Databricks notebook source
db_notebooks = {"all":["/deltalake/connections/Mount_Points",
                       "/deltalake/cleaning/Cleaning_Airport",
                       "/deltalake/cleaning/Cleaning_Api_Data",
                       "/deltalake/cleaning/Cleaning_Flight",
                       "/deltalake/cleaning/Cleaning_Planes",
                       "/deltalake/Transformations/Delay_flights_per_airport"
],
                "cleaning":["/deltalake/cleaning/Cleaning_Airport",
                       "/deltalake/cleaning/Cleaning_Api_Data",
                       "/deltalake/cleaning/Cleaning_Flight",
                       "/deltalake/cleaning/Cleaning_Planes"

                ],
                "mount_point":["/deltalake/connections/Mount_Points"],
                "transformation":["/deltalake/Transformations/Delay_flights_per_airport"]
                }

# COMMAND ----------

if dbutils.widgets.get("process_to_run") not in db_notebooks:
    raise Exception(f"Given process {dbutils.widgets.get('process_to_run')} Does Not Exists")

for notebook in db_notebooks[dbutils.widgets.get("process_to_run")]:
   try:
     dbutils.notebook.run(notebook, 300)
   except Exception as e:
    if "Mount_Points" in notebook:
        # Handle the WorkflowException here
        pass
    else:
        raise Exception("An unexpected error occurred") from e

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table deltadb.app_view
