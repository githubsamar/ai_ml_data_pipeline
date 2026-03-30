
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('all-MiniLM-L6-v2')



# Text Embedding Generation  
def generate_embedding(text):
    return model.encode(text)

#add embedding to df
def add_embeddings(df):
    df["combined_text"] = df["title"] + " " + df["summary"]
    df["embedding"] = df["combined_text"].apply(generate_embedding)
    return df



# Vector Similarity Search  function
def find_similar_articles(query, df, top_k=5):
    query_emb = generate_embedding(query)

    similarities = cosine_similarity(
        [query_emb],
        list(df["embedding"])
    )[0]

    df["similarity"] = similarities
    return df.nlargest(top_k, "similarity")[["article_id", "similarity"]]