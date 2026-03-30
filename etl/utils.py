
import pandas as pd
from fuzzywuzzy import process


def validate_company_names(df, metadata_df):
    valid_names = metadata_df["company_name"].unique()

    def match(name):
        if name in valid_names:
            return name

        match, score = process.extractOne(name, valid_names)
        return match if score > 80 else None

    df["matched_company"] = df["company_name"].apply(match)
    df["company_valid"] = df["matched_company"].notna()

    return df
    
    
#Export function to Csv with embeddings
def export_csv(df):
    df.to_csv("./data/output_data/ai_articles_enriched.csv", index=False)