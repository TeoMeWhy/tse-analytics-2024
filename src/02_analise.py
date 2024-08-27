# %%

import pandas as pd
import sqlalchemy
import matplotlib.pyplot as plt
import matplotlib.image as img
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import seaborn as sn
from adjustText import adjust_text


# %%

with open("partidos.sql", "r") as open_file:
    query = open_file.read()

engine = sqlalchemy.create_engine("sqlite:///../data/database.db")

df = pd.read_sql_query(query, engine)
df.head()

# %%

txGenFeminino = df["totalGenFeminino"].sum() / df["totalCandidaturas"].sum()
txCorRacaPreta = df["totalCorRacaPreta"].sum() / df["totalCandidaturas"].sum()
txCorRacaNaoBranca = df["totalCorRacaNaoBranca"].sum() / df["totalCandidaturas"].sum()
txCorRacaPretaParda = df["totalCorRacaPretaParda"].sum() / df["totalCandidaturas"].sum()

# %%

plt.figure(dpi=360, figsize=(6,5.5))

sn.scatterplot(data=df,
               x="txGenFemininoBR",
               y="txCorRacaPretaBR",
               )

texts = []
for i in df['SG_PARTIDO']:
    data = df[df['SG_PARTIDO'] == i]
    x = data['txGenFemininoBR'].values[0]
    y = data['txCorRacaPretaBR'].values[0]
    texts.append(plt.text(x, y, i, fontsize=9))

adjust_text(texts,
            force_points=0.0002,
            force_text=0.4,
            expand_points=(0.5, 0.75), expand_text=(0.5, 0.75),
            arrowprops=dict(arrowstyle="-", color='black', lw=0.2),
            pull_threshold=1000,
            )

plt.grid(True)
plt.title("Partidos: Cor vs Genero - Eleições 2024")
plt.xlabel("Taxa de Mulheres")
plt.ylabel("Taxa de Pessoas Pretas")

plt.hlines(y=txCorRacaPreta, xmin=0.3, xmax=0.55, colors='black', alpha=0.6, linestyles='--', label=f"Pessoas Pretas Geral: {100*txCorRacaPreta:.0f}%")
plt.vlines(x=txGenFeminino, ymin=0.05, ymax=0.35, colors='tomato', alpha=0.6, linestyles='--', label=f"Mulheres Geral: {100*txGenFeminino:.0f}%")

plt.legend()

logo = img.imread("../img/logo.png")
imagebox = OffsetImage(logo, zoom=0.035, alpha=0.6)
ab = AnnotationBbox(imagebox, (0.8, 0.43),
                    frameon=False,
                    pad=0,
                    xycoords='axes fraction',
                    boxcoords="axes fraction",
                    box_alignment=(0.5, 0.5))

plt.gca().add_artist(ab)

plt.savefig("../img/partidos_cor_raca_genero.png")


# %%

plt.figure(dpi=360, figsize=(6,5.5))

sn.scatterplot(data=df,
               x="txGenFemininoBR",
               y="txCorRacaPretaBR",
               size="totalCandidaturas",
               sizes=(5,300))

texts = []
for i in df['SG_PARTIDO']:
    data = df[df['SG_PARTIDO'] == i]
    x = data['txGenFemininoBR'].values[0]
    y = data['txCorRacaPretaBR'].values[0]
    texts.append(plt.text(x, y, i, fontsize=9))

adjust_text(texts,
            force_points=0.0002,
            force_text=0.4,
            expand_points=(0.5, 0.75), expand_text=(0.5, 0.75),
            arrowprops=dict(arrowstyle="-", color='black', lw=0.2),
            pull_threshold=1000,
            )

plt.grid(True)
plt.title("Partidos: Cor vs Genero - Eleições 2024")
plt.xlabel("Taxa de Mulheres")
plt.ylabel("Taxa de Pessoas Pretas")

plt.hlines(y=txCorRacaPreta, xmin=0.3, xmax=0.55, colors='black', alpha=0.6, linestyles='--', label=f"Pessoas Pretas Geral: {100*txCorRacaPreta:.0f}%")
plt.vlines(x=txGenFeminino, ymin=0.05, ymax=0.35, colors='tomato', alpha=0.6, linestyles='--', label=f"Mulheres Geral: {100*txGenFeminino:.0f}%")

handles, labels = plt.gca().get_legend_handles_labels()
handles = handles[5:]
labels = labels[5:]  

plt.legend(handles=handles, labels=labels)

logo = img.imread("../img/logo.png")
imagebox = OffsetImage(logo, zoom=0.035, alpha=0.6)
ab = AnnotationBbox(imagebox, (0.8, 0.43),
                    frameon=False,
                    pad=0,
                    xycoords='axes fraction',
                    boxcoords="axes fraction",
                    box_alignment=(0.5, 0.5))

plt.gca().add_artist(ab)

plt.savefig("../img/partidos_cor_raca_genero_bolha_size.png")

# %%

from sklearn import cluster

X = df[["txGenFemininoBR","txCorRacaPretaBR"]]
model = cluster.KMeans(n_clusters=6)
model.fit(X)

df["clusterBR"] = model.labels_

plt.figure(dpi=360, figsize=(6,5.5))

sn.scatterplot(data=df,
               x="txGenFemininoBR",
               y="txCorRacaPretaBR",
            #    size="totalCandidaturas",
            #    sizes=(5,300),
               hue='clusterBR',
               palette='viridis',
               )

texts = []
for i in df['SG_PARTIDO']:
    data = df[df['SG_PARTIDO'] == i]
    x = data['txGenFemininoBR'].values[0]
    y = data['txCorRacaPretaBR'].values[0]
    texts.append(plt.text(x, y, i, fontsize=9))

adjust_text(texts,
            force_points=0.0002,
            force_text=0.4,
            expand_points=(0.5, 0.75), expand_text=(0.5, 0.75),
            arrowprops=dict(arrowstyle="-", color='black', lw=0.2),
            pull_threshold=1000,
            )

plt.grid(True)
plt.title("Partidos: Cor vs Genero - Eleições 2024")
plt.xlabel("Taxa de Mulheres")
plt.ylabel("Taxa de Pessoas Pretas")

plt.hlines(y=txCorRacaPreta, xmin=0.3, xmax=0.55, colors='black', alpha=0.6, linestyles='--', label=f"Pessoas Pretas Geral: {100*txCorRacaPreta:.0f}%")
plt.vlines(x=txGenFeminino, ymin=0.05, ymax=0.35, colors='tomato', alpha=0.6, linestyles='--', label=f"Mulheres Geral: {100*txGenFeminino:.0f}%")

handles, labels = plt.gca().get_legend_handles_labels()
handles = handles[6:]
labels = labels[6:]

# Add your custom legend entries (if needed)
plt.legend(handles=handles, labels=labels)

logo = img.imread("../img/logo.png")
imagebox = OffsetImage(logo, zoom=0.035, alpha=0.6)
ab = AnnotationBbox(imagebox, (0.8, 0.43),
                    frameon=False,
                    pad=0,
                    xycoords='axes fraction',
                    boxcoords="axes fraction",
                    box_alignment=(0.5, 0.5))

plt.gca().add_artist(ab)

plt.savefig("../img/partidos_cor_raca_genero_clusterBR.png")
# %%


plt.figure(dpi=360, figsize=(6,5.5))

sn.scatterplot(data=df,
               x="txGenFemininoBR",
               y="txCorRacaPretaBR",
               size="totalCandidaturas",
               sizes=(5,300),
               hue='clusterBR',
               palette='viridis',
)

texts = []
for i in df['SG_PARTIDO']:
    data = df[df['SG_PARTIDO'] == i]
    x = data['txGenFemininoBR'].values[0]
    y = data['txCorRacaPretaBR'].values[0]
    texts.append(plt.text(x, y, i, fontsize=9))

adjust_text(texts,
            force_points=0.0002,
            force_text=0.4,
            expand_points=(0.5, 0.75), expand_text=(0.5, 0.75),
            arrowprops=dict(arrowstyle="-", color='black', lw=0.2),
            pull_threshold=1000,
            )

plt.grid(True)
plt.suptitle("Partidos: Cor vs Genero - Eleições 2024")
plt.title("Maior a bolha, maior o tamanho do partido")
plt.xlabel("Taxa de Mulheres")
plt.ylabel("Taxa de Pessoas Pretas")

plt.hlines(y=txCorRacaPreta, xmin=0.3, xmax=0.55, colors='black', alpha=0.6, linestyles='--', label=f"Pessoas Pretas Geral: {100*txCorRacaPreta:.0f}%")
plt.vlines(x=txGenFeminino, ymin=0.05, ymax=0.35, colors='tomato', alpha=0.6, linestyles='--', label=f"Mulheres Geral: {100*txGenFeminino:.0f}%")

handles, labels = plt.gca().get_legend_handles_labels()
handles = handles[13:]
labels = labels[13:]

plt.legend(handles=handles, labels=labels)

logo = img.imread("../img/logo.png")
imagebox = OffsetImage(logo, zoom=0.035, alpha=0.6)
ab = AnnotationBbox(imagebox, (0.8, 0.43),
                    frameon=False,
                    pad=0,
                    xycoords='axes fraction',
                    boxcoords="axes fraction",
                    box_alignment=(0.5, 0.5))

plt.gca().add_artist(ab)
