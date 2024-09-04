# Databricks notebook source
def import_query(path):
    with open(path) as open_file:
        return open_file.read()

catalog = 'gold'
database = 'tse'
table = "profile_partidos"
tablename = f"{catalog}.{database}.{table}"

query = import_query(f"{table}.sql")

# COMMAND ----------

# DBTITLE 1,EXECUÇÃO
df = spark.sql(query)

# COMMAND ----------

# DBTITLE 1,INGESTÃO
(df.coalesce(1)
   .write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable(tablename))
