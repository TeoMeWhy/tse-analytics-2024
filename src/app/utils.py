import matplotlib.pyplot as plt
import seaborn as sn
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
from adjustText import adjust_text

from sklearn import cluster
from sklearn import preprocessing

def make_scatter(data, x, y, x_label, y_label, cluster=False, size=False):

    config = {
        "data":data,
        "x":x,
        "y":y,
        "size":"totalCandidaturas",
        "sizes":(5,300),
        "hue":'cluster',
        "palette":'viridis',
        "alpha":0.6,
    }

    if not cluster:
        del config['hue']
        del config["palette"]

    if not size:
        del config['size']
        del config['sizes']

    fig = plt.figure(dpi=360, figsize=(6,5.5))

    sn.scatterplot(**config)

    texts = []
    for i in data['descSiglaPartido']:
        data_tmp = data[data['descSiglaPartido'] == i]
        x_pos = data_tmp[x].values[0]
        y_pos = data_tmp[y].values[0]
        texts.append(plt.text(x_pos, y_pos, i, fontsize=9))

    adjust_text(texts,
                force_points=0.0002,
                force_text=0.4,
                expand_points=(0.5, 0.75), expand_text=(0.5, 0.75),
                arrowprops=dict(arrowstyle="-", color='black', lw=0.2),
                pull_threshold=1000,
                )

    plt.grid(True)
    plt.suptitle(f"{x_label.title()} vs {y_label.title()} - Eleições 2024")
    
    if size:
        plt.title("Maior a bolha, maior o tamanho do partido", fontdict={"size":9})
    
    plt.xlabel(x_label.title())
    plt.ylabel(y_label.title())

    if x == 'txGenFeminino' and y == 'txCorRacaPreta':
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
    
    else:
        legend = plt.legend()
        legend.remove()

    return fig

def make_clusters(data, features, n=6):
    norm = preprocessing.MinMaxScaler()
    data_norm = norm.fit_transform(data[features])
    model = cluster.KMeans(n_clusters=n, random_state=42, max_iter=10000)
    model.fit(data_norm)
    data["cluster"] = model.labels_
    return data