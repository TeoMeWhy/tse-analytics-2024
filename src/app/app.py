# %%
import os

import streamlit as st
import pandas as pd
import sqlalchemy
import dotenv

import gdown

from utils import make_scatter, make_clusters

dotenv.load_dotenv(".env")

DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_CATALOG = os.getenv("DATABRICKS_CATALOG")


app_path = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.dirname(app_path)
base_path = os.path.dirname(src_path)
data_path = os.path.join(base_path, "data")

@st.cache_data(ttl=60*60*24)
def create_df():
    url = f"databricks://token:{DATABRICKS_TOKEN}@{DATABRICKS_SERVER_HOSTNAME}?http_path={DATABRICKS_HTTP_PATH}&catalog={DATABRICKS_CATALOG}&schema=gold"
    engine = sqlalchemy.create_engine(url)
    df = pd.read_sql("select * from gold.tse.profile_partidos", engine)
    print(df)
    return df


features_map = {
    "PERCENTUAL FEMININO": "txGenFeminino",
    "PERCENTUAL RACA PRETA": "txCorRacaPreta",
    "PERCENTUAL RACA PRETA PARDA": "txCorRacaPretaParda",
    "PERCENTUAL RACA NÃO-BRANCA": "txCorRacaNaoBranca",
    "MEDIA BENS TOTAL": "avgBens",
    "MEDIA BENS SEM ZEROS": "avgBensNotZero",
    "PERCENTUAL ESTADO CIVIL CASADO(A)": "txEstadoCivilCasado",
    "PERCENTUAL ESTADO CIVIL SOLTEIRO(A)": "txEstadoCivilSolteiro",
    "PERCENTUAL ESTADO CIVIL DIVORCIADO(A)": "txEstadoCivilSeparadoDivorciado",
    "MÉDIA IDADE": "avgIdade",
}

features_options = list(features_map.keys())
features_options.sort()

# %%

df = create_df()

welcome = """
# TSE Analytics - Eleições 2024

Altere as opções abaixo para ver as mudanças no gráfico.
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

## Definição do estado e cargo
col1, col2, = st.columns(2)
with col1:
    estado = st.selectbox(label="Estado", placeholder="Selecione o estado para filtro", options=uf_options)

with col2:
    cargo = st.selectbox(label="Cargo", placeholder="Selecione o cargo para filtro", options=cargos_options)


## Definição dos eixos
col1, col2, = st.columns(2)
with col1:
    x_option = st.selectbox(label="Eixo x", options=features_options, index=6)
    x = features_map[x_option]
    new_features_options = features_options.copy()
    new_features_options.remove(x_option)

with col2:
    y_option = st.selectbox(label="Eixo y", options=new_features_options, index=7)
    y = features_map[y_option]

size = st.checkbox("Tamanho das bolhas")

# Definição do uso de cluster
col1, col2, = st.columns(2)
with col1:
    cluster = st.checkbox("Definir cluster")
    if cluster:
        n_cluster = st.number_input("Quantidade de clusters", value=6, format="%d", max_value=10, min_value=1)

with col2:
    if cluster:
        features_options_selected = st.multiselect(label="Variáveis para agrupamento",
                                                   options=features_options,
                                                   default=["PERCENTUAL FEMININO","PERCENTUAL RACA PRETA"])
        features_selected = [features_map[i] for i in features_options_selected]


data = df[(df['SG_UF']==estado) & (df['DS_CARGO']==cargo) & (df['SG_PARTIDO']!='GERAL')].copy()

total_candidatos = int(data["totalCandidaturas"].sum())
st.markdown(f"Total de candidaturas: {total_candidatos}")

if cluster:
    data = make_clusters(data=data, features=features_selected, n=n_cluster)

fig = make_scatter(data,
                   x=x, y=y,
                   x_label=x_option, y_label=y_option,
                   cluster=cluster,
                   size=size)

st.pyplot(fig)

st.markdown("""
### Créditos            

Uma iniciativa [Téo Me Why](github.com/teomewhy) em conjunto com a comunidade de análise e ciência de dados ao vivo!

Você pode conferir o repositório deste projeto aqui: [github.com/TeoMeWhy/tse-analytics-2024](https://github.com/TeoMeWhy/tse-analytics-2024).
""")