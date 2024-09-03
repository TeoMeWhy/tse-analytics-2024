# Databricks notebook source
import os
import delta
import json

with open("source.json", "r") as open_file:
    sources = json.load(open_file)

fonte = dbutils.widgets.get("fonte")
partition = sources[fonte] ["partition"]

volume_path = '/Volumes/raw/tse/full_load'
folder_path = f'{volume_path}/{fonte}'
files_path = f'{folder_path}/*BRASIL.csv'

df = spark.read.csv(files_path,
                    sep=';',
                    header=True,
                    encoding='latin1',
                    inferSchema=True,
                    multiLine=True)

# COMMAND ----------

(df.write
   .mode("overwrite")
   .format("delta")
   .partitionBy(partition)
   .option("overwriteSchema", "true")
   .saveAsTable(f"bronze.tse.{fonte}"))

delta_table = delta.DeltaTable.forName(spark, f"bronze.tse.{fonte}")
delta_table.vacuum()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.tse.consulta_cand
