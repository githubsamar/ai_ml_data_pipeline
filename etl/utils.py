
#Export function to Csv with embeddings
def export_csv(df):
    df.to_csv("./data/output_data/ai_articles_enriched.csv", index=False)
    