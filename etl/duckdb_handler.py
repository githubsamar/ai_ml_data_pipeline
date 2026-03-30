import duckdb
from sklearn.metrics.pairwise import cosine_similarity



# Function to load df to DuckDB table
def load_to_duckdb(df):
    con = duckdb.connect("articles.db")
    con.execute("CREATE TABLE IF NOT EXISTS articles AS SELECT * FROM df")
    return con

# function for hybrid search: SQL filters + vector similarity
def query_duckdb(con, query):
    return con.execute(query).fetchdf()

# define filter function
def filter_ai_articles(df):

    df = df[
        (
            (df["category"] == "AI_ML") |
            (df["industry"].str.contains("AI", case=False))
        ) &
        (df["year"].between(2022, 2024)) &
        (df["revenue_usd"] >= 50_000_000)
    ]
    
    df = df[[ 'article_id', 'title', 'company_name', 'published_date', 'category', 
            'revenue_usd', 'year', 'summary', 'url', 'industry', 'founded_year', 'headquarters', 
            'employee_count', 'is_public', 'stock_ticker', 'company_age', 'company_size_category' , 
            'embedding', 'top_similar_articles']]
    return df


def add_top_similar(df):
    embeddings = list(df["embedding"])

    sim_matrix = cosine_similarity(embeddings)

    top_similar = []
    for i in range(len(df)):
        sims = list(enumerate(sim_matrix[i]))
        sims = sorted(sims, key=lambda x: x[1], reverse=True)[1:4]
        top_similar.append([df.iloc[j]["article_id"] for j, _ in sims])

    df["top_similar_articles"] = top_similar
    return df