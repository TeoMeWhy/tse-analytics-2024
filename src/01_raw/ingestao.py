# Databricks notebook source
# DBTITLE 1,SETUP
import os
import requests
import zipfile
import json

with open("sources.json", "r") as open_file:
    sources = json.load(open_file)

ano = dbutils.widgets.get("ano")
fonte = dbutils.widgets.get("fonte")

volume_path = "/Volumes/raw/tse/full_load/"
folder_path = os.path.join(volume_path, fonte)

os.makedirs(folder_path, exist_ok=True)

url = sources[ano][fonte]
filename = url.split("/")[-1]

file_path = os.path.join(folder_path, filename)

# COMMAND ----------

# DBTITLE 1,DOWNLOAD
resp = requests.get(url)

# COMMAND ----------

# DBTITLE 1,ESCRITA
with open(file_path, "wb") as open_file:
    open_file.write(resp.content)

# COMMAND ----------

# DBTITLE 1,UNZIP
with zipfile.ZipFile(file_path, 'r') as zip_ref:
    zip_ref.extractall(folder_path)

# COMMAND ----------

df = spark.read.csv(f"{folder_path}/*BRASIL.csv", sep=";", header=True, encoding='latin1')
df.display()
