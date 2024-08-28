# %%
import streamlit as st
import pandas as pd
import sqlalchemy

import os

import gdown

from utils import make_scatter, make_clusters

app_path = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.dirname(app_path)
base_path = os.path.dirname(src_path)
data_path = os.path.join(base_path, "data")

@st.cache_data(ttl=60*60*24)
def create_df():
    filename = os.path.join(data_path, "data_partidos.parquet")
    return pd.read_parquet(filename)


# %%

df = create_df()

welcome = """
# TSE Analytics - Eleições 2024

Uma iniciativa [Téo Me Why](github.com/teomewhy) em conjunto com a comunidade de análise e ciência de dados ao vivo!

Você pode conferir o repositório deste projeto aqui: [github.com/TeoMeWhy/tse-analytics-2024](https://github.com/TeoMeWhy/tse-analytics-2024).

### Diversidade

Como primeira análise dos partidos, focamos na representatividade de mulheres e pessoas pretas nas candidaturas.
"""

st.markdown(welcome)

uf_options = df["SG_UF"].unique().tolist()
uf_options.sort()
uf_options.remove("BR")
uf_options = ["BR"] + uf_options

cargos_options = df["DS_CARGO"].unique().tolist()
cargos_options.sort()
cargos_options.remove("GERAL")
cargos_options = ["GERAL"] + cargos_options

col1, col2, = st.columns(2)

with col1:
    estado = st.selectbox(label="Estado", placeholder="Selecione o estado para filtro", options=uf_options)
    size = st.checkbox("Tamanho das bolhas")
    cluster = st.checkbox("Definir cluster")

with col2:
    cargo = st.selectbox(label="Cargo", placeholder="Selecione o cargo para filtro", options=cargos_options)
    st.markdown("")
    n_cluster = st.number_input("Quantidade de clusters", value=6, format="%d", max_value=10, min_value=1)


data = df[(df['SG_UF']==estado) & (df['DS_CARGO']==cargo)].copy()

total_candidatos = int(data["totalCandidaturas"].sum())
st.markdown(f"Total de candidaturas: {total_candidatos}")

if cluster:
    data = make_clusters(data, n_cluster)

fig = make_scatter(data, cluster=cluster, size=size)

st.pyplot(fig)

