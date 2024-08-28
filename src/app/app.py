# %%
import streamlit as st
import pandas as pd
import sqlalchemy

import os


from utils import make_scatter, make_clusters

app_path = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.dirname(app_path)
base_path = os.path.dirname(src_path)
data_path = os.path.join(base_path, "data")

database_path = os.path.join(data_path, "database.db")
engine = sqlalchemy.create_engine(f"sqlite:///{database_path}")


query_path = os.path.join(app_path, "etl_partidos.sql")
with open(query_path, "r") as open_file:
    query = open_file.read()

df = pd.read_sql(query, engine)

# %%

welcome = """
# TSE Analytics - Eleições 2024

Uma iniciativa [Téo Me Why](github.com/teomewhy) em conjunto com a comunidade de análise e ciência de dados ao vivo!

Você pode conferir o repositório deste projeto aqui: [github.com/TeoMeWhy/tse-analytics-2024](https://github.com/TeoMeWhy/tse-analytics-2024).

### Diversidade

Como primeira análise dos partidos, focamos na representatividade de mulheres e pessoas pretas nas candidaturas.
"""

st.markdown(welcome)

uf_options = df["SG_UF"].unique().tolist()
uf_options.remove("BR")
uf_options = ["BR"] + uf_options



col1, col2, = st.columns(2)

with col1:
    estado = st.selectbox(label="Estado", placeholder="Selecione o estado para filtro", options=uf_options)
    cluster = st.checkbox("Definir cluster")

with col2:
    size = st.checkbox("Tamanho das bolhas")
    n_cluster = st.number_input("Quantidade de clusters", value=6, format="%d", max_value=10, min_value=1)


data = df[df['SG_UF']==estado].copy()

if cluster:
    data = make_clusters(data, n_cluster)

fig = make_scatter(data, cluster=cluster, size=size)

st.pyplot(fig)

