# Databricks notebook source
#Create Mount Point for adlsg2 raw layer
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "14d976df-4267-4fd0-8afb-05682a022d5e",
          "fs.azure.account.oauth2.client.secret": "lsX8Q~J2q_U0S8w5r6KP2-LKgT6DSyxDUUkSvddC",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/8aa62754-c26c-41ca-b730-a3cb74011794/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://delta-air-raw-zone@deltaairlinedatalake.dfs.core.windows.net/",
  mount_point = "/mnt/raw_zone",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/raw_zone

# COMMAND ----------

#Create Mount Point for adlsg2 cleaned layer
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "14d976df-4267-4fd0-8afb-05682a022d5e",
          "fs.azure.account.oauth2.client.secret": "lsX8Q~J2q_U0S8w5r6KP2-LKgT6DSyxDUUkSvddC",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/8aa62754-c26c-41ca-b730-a3cb74011794/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://delta-air-cleaned-zone@deltaairlinedatalake.dfs.core.windows.net/",
  mount_point = "/mnt/cleaned_zone",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/cleaned_zone
