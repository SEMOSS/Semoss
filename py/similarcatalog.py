from scipy.spatial import distance
import re
from sentence_transformers import SentenceTransformer
import pandas as pd
from scipy.spatial import distance
import numpy as np


def build_domain(topN=5, cur_model='sentence-transformers/msmarco-distilbert-base-v4'):
    desc = []
    titles = []
    model = SentenceTransformer(cur_model)
    for k, v in metadatas.items():
        df = get_dataset_focus(v, model)
        titles.append(k)
        if topN == 0:
            cnt = len(df)
        else:
            cnt = min(topN, len(df))
        desc.append(' '.join(df.iloc[:cnt, 0]))
    domain = pd.DataFrame({'Title': titles})
    domain['Description'] = desc
    return domain


def get_dataset_focus(metadata, model):
    items = metadata
    items = [re.sub("[.,_-]", " ", item).split(' ') for item in items]
    multi_words = [item for item in items]
    words = [item for elem in multi_words for item in elem]
    words = list(set(words))
    out = derive_words_focus(words, model)
    return out


def derive_words_focus(words, model):
    words_embeddings = model.encode(words)
    obj_embeddings = model.encode(' '.join(words))
    matches = []
    for i in range(len(words_embeddings)):
        matches.append(
            round(1-distance.cosine(words_embeddings[i], obj_embeddings), 4))
    data = {'Word': words, 'Similarity': matches}
    out = pd.DataFrame(data)
    out = out.sort_values(by='Similarity', ascending=False)
    return out

# Finish building domain methods, encode catalog methods


def encode_catalog(domain, cur_model='sentence-transformers/msmarco-distilbert-base-v4'):
    model = SentenceTransformer(cur_model)
    titles = domain.iloc[:, 0]
    desc = domain.iloc[:, 1].tolist()
    # Encoding:
    desc_embeddings = model.encode(desc)
    bag = {'model': model, 'catalog': desc_embeddings, 'titles': titles}
    return bag


# Get similar catalogs
def get_sim_datasets(title, bag, threshold=0.1):
    embeddings = bag['catalog']
    titles = list(bag['titles'])

    try:
        ind = titles.index(title)
    except:
        ind = -1
        return 'Dataset name not in the catalog'
    if ind >= 0:
        search_embeddings = embeddings[ind]
        embeddings = np.delete(embeddings, ind, 0)
        titles.remove(title)

    matches = []
    for i in range(len(embeddings)):
        matches.append(
            round(1-distance.cosine(embeddings[i], search_embeddings), 4))
    data = {'Title': titles, 'Similarity': matches}
    out = pd.DataFrame(data)
    out = out.sort_values(by='Similarity', ascending=False)
    out = out[out['Similarity'] >= threshold]
    return out


# run the methods above
domain = build_domain()
bag = encode_catalog(domain)
out = get_sim_datasets(alias, bag)

if len(out) != 0:
    has_results = True
    result = out['Title'].values.tolist()
else:
	result = []
