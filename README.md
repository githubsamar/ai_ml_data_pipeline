# ai_ml_data_pipeline

# AI Articles ETL Pipeline

## Setup

- Step1.  Download and unzip  data_pipeline_project.zip
- Step2.  Create virtual environment venv:   python3 -m venv venv
- Step3.  Activate venv : venv\Scripts\activate
- Step4.  Install requirements : pip install -r requirements.txt

## Run pipeline
python pipeline.py


## Folder/SubFolders for iput/output data : 
./data/iput_data, ./data/output_data 

# Input files: 
company_metadata.json, tech_news.csv


# Output file
ai_articles_enriched.csv under ./data/output_data 

## Features
- Data cleaning (revenue, date, category)
- Metadata enrichment
- Embeddings + similarity search
- DuckDB hybrid querying

## Example: use test.ipynb for testing
- similar article search:
```python
from embeddings import find_similar_articles
find_similar_articles("AI startup funding", df, 5)
```
- Search using DuckDB

```python
con=load_to_duckdb(df)
query_result = con.sql("""
    SELECT *
    FROM articles
    WHERE revenue_usd >= 50000000
    AND year BETWEEN 2022 AND 2024
    AND (category = 'AI_ML' OR industry = 'AI/ML')
""")
result_df = query_result.df()
print(result_df)
con.close()
```