# Databricks notebook source
# DBTITLE 1,SETUP
catalog = 'silver'
database = 'tse'
table = dbutils.widgets.get("table")

tablename = f'{catalog}.{database}.{table}'

with open(f'{table}.sql', 'r') as open_file:
    query = open_file.read()


# COMMAND ----------

# DBTITLE 1,EXECUÇÃO
df = spark.sql(query)

# COMMAND ----------

# DBTITLE 1,SAVE
(df.coalesce(1)
   .write
   .mode("overwrite")
   .format("delta")
   .option("overwriteSchema", "true")
   .partitionBy("idEleicao")
   .saveAsTable(tablename))
