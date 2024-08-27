# %%
import json

import pandas as pd
import sqlalchemy
# %%

engine = sqlalchemy.create_engine("sqlite:///../data/database.db")

with open("ingestoes.json", "r") as open_file:
    ingestoes = json.load(open_file)


for i in ingestoes:
    df = pd.read_csv(i['path'], encoding='latin-1', sep=";")
    df.to_sql(i["table"], engine, if_exists="replace", index=False)
