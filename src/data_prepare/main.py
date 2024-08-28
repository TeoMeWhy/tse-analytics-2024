import os
import pandas as pd
import sqlalchemy

prepare_path = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.dirname(prepare_path)
base_path = os.path.dirname(src_path)
data_path = os.path.join(base_path, "data")


database_path = os.path.join(data_path, "database_gd.db")
engine = sqlalchemy.create_engine(f"sqlite:///{database_path}")

query_path = os.path.join(prepare_path, "etl_partidos.sql")
with open(query_path, "r") as open_file:
    query = open_file.read()

df = pd.read_sql(query, engine)

filename = os.path.join(data_path, "data_partidos.parquet")
df.to_parquet(filename, index=False)