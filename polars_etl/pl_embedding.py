from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import polars as pl

model = SentenceTransformer("all-MiniLM-L6-v2")


def generate_embeddings(df):
    texts = (df["title"] + " " + df["summary"]).to_list()
    embeddings = model.encode(texts, show_progress_bar=True)

    return df.with_columns([
        pl.Series("embedding", embeddings.tolist())
    ])


def compute_top_similar(df):
    embeddings = np.array(df["embedding"].to_list())
    sim_matrix = cosine_similarity(embeddings)

    top_similar = []
    article_ids = df["article_id"].to_list()

    for i in range(len(article_ids)):
        scores = list(enumerate(sim_matrix[i]))
        scores = sorted(scores, key=lambda x: x[1], reverse=True)[1:4]
        ids = [article_ids[j] for j, _ in scores]
        top_similar.append(ids)

    return df.with_columns([
        pl.Series("top_similar_articles", top_similar)
    ])