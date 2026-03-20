import polars as pl
import json

from pl_cleaning import *
from pl_embedding import *
from polars_etl.pl_duckdb_utils import *


def run_pipeline():

    # -----------------------------
    # Load Data (Polars)
    # -----------------------------
    df = pl.read_csv(r"C:\Users\samar\ai_ml_data_pipeline\data\input_data\tech_news.csv")

    with open(r"C:\Users\samar\ai_ml_data_pipeline\data\input_data\company_metadata.json") as f:
        metadata =json.load(f)

    metadata = pl.from_dicts(
        [{"company_name": k, **v} for k, v in metadata.items()]
    )

    # -----------------------------
    # Cleaning
    # -----------------------------
    df = df.with_columns([
        pl.col("revenue").map_elements(clean_revenue).alias("revenue_usd"),
        pl.col("published_date").map_elements(normalize_date).alias("published_date"),
        pl.col("category").map_elements(standardize_category).alias("category"),
    ])

    # Date parts
    df = df.with_columns([
        pl.col("published_date").dt.year().alias("year"),
        pl.col("published_date").dt.month().alias("month"),
        pl.col("published_date").dt.quarter().alias("quarter"),
    ])

    # -----------------------------
    # Join Metadata
    # -----------------------------
    df = df.join(
        metadata,
        on="company_name",
        how="left"
    )

    # -----------------------------
    # Derived Fields
    # -----------------------------
    df = df.with_columns([
        (pl.col("year") - pl.col("founded_year")).alias("company_age"),
        pl.when(pl.col("employee_count") < 10000)
          .then("Small")
          .when(pl.col("employee_count") <= 30000)
          .then("Medium")
          .otherwise("Large")
          .alias("company_size_category")
    ])

    # -----------------------------
    # Embeddings
    # -----------------------------
    df = generate_embeddings(df)
    df = compute_top_similar(df)

    # -----------------------------
    # DuckDB Hybrid Query
    # -----------------------------
    con = load_to_duckdb(df)

    query = """
    SELECT *
    FROM articles
    WHERE 
        (category = 'AI_ML' OR industry LIKE '%AI%')
        AND year BETWEEN 2022 AND 2024
        AND revenue_usd >= 50000000
    """

    result = query_duckdb(con, query)

    # -----------------------------
    # Export
    # -----------------------------
    pl.from_pandas(result).write_csv("ai_articles_enriched_pl.csv")

    print("✅ Pipeline completed!")


if __name__ == "__main__":
    run_pipeline()