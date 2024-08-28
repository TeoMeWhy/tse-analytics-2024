import matplotlib.pyplot as plt
import seaborn as sn
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
from adjustText import adjust_text

from sklearn import cluster

def make_scatter(data, cluster=False, size=False):

    config = {
        "data":data,
        "x":"txGenFeminino",
        "y":"txCorRacaPreta",
        "size":"totalCandidaturas",
        "sizes":(5,300),
        "hue":'cluster',
        "palette":'viridis',
        "alpha":0.6,
    }

    if not cluster:
        del config['hue']

    if not size:
        del config['size']
        del config['sizes']

    fig = plt.figure(dpi=360, figsize=(6,5.5))

    sn.scatterplot(**config)

    texts = []
    for i in data['SG_PARTIDO']:
        data_tmp = data[data['SG_PARTIDO'] == i]
        x = data_tmp['txGenFeminino'].values[0]
        y = data_tmp['txCorRacaPreta'].values[0]
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
    
    if size:
        plt.title("Maior a bolha, maior o tamanho do partido", fontdict={"size":9})
    
    plt.xlabel("Taxa de Mulheres")
    plt.ylabel("Taxa de Pessoas Pretas")

    txCorRacaPreta = data['totalCorRacaPreta'].sum() / data['totalCandidaturas'].sum()
    txGenFeminino = data['totalGenFeminino'].sum() / data['totalCandidaturas'].sum()

    plt.hlines(y=txCorRacaPreta,
               xmin=data['txGenFeminino'].min(),
               xmax=data['txGenFeminino'].max(), colors='black', alpha=0.6, linestyles='--', label=f"Pessoas Pretas Geral: {100*txCorRacaPreta:.0f}%")
    
    plt.vlines(x=txGenFeminino,
               ymin=data['txCorRacaPreta'].min(),
               ymax=data['txCorRacaPreta'].max(), colors='tomato', alpha=0.6, linestyles='--', label=f"Mulheres Geral: {100*txGenFeminino:.0f}%")

    handles, labels = plt.gca().get_legend_handles_labels()
    handles = handles[-2:]
    labels = labels[-2:]

    plt.legend(handles=handles, labels=labels)

    return fig

def make_clusters(data, n=6):
    model = cluster.KMeans(n_clusters=n, random_state=42, max_iter=1000)
    model.fit(data[['txGenFeminino', 'txCorRacaPreta']])
    data["cluster"] = model.labels_
    return data