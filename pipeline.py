import pandas as pd
from etl.enrichment import *
from etl.embeddings import *
from etl.duckdb_handler import *
from etl.utils import *
from etl.cleaningdata import *
import warnings
warnings.filterwarnings("ignore")
import polars as pl
import json
from etl.data_validation import *

def run_pipeline():
    
    # -----------------------------------------------------
    # Loading data from ./data/input_data/ using pandas
    # -----------------------------------------------------
    ## read source data from csv and json using pandas
    df = pd.read_csv("./data/input_data/tech_news.csv")
    metadata = pd.read_json("./data/input_data/company_metadata.json",  orient='index').reset_index()
    metadata.rename(columns={'index': 'company_name'}, inplace=True)
    
    
    # -------------------------------------------------
    # # Loading data from ./data/input_data/ 
    # using polars for large data sets
    # -------------------------------------------------
    ## read source data from csv and json using polars
    '''
    df = pl.read_csv("./data/input_data/tech_news.csv")
    df = df.to_pandas()

    with open("./data/input_data/company_metadata.json") as f:
        metadata =json.load(f)

    metadata = pl.from_dicts(
        [{"company_name": k, **v} for k, v in metadata.items()]
    )
    metadata = metadata.to_pandas()
    '''
    
    # -----------------------------------------------------   
    #  Clean and format the "revenue" column
    # -----------------------------------------------------
    df["revenue_usd"] = df["revenue"].apply(parse_revenue)
    
    # -----------------------------------------------------
    #  Clean and normalize the "published_date" column:
    # -----------------------------------------------------
    df["published_date"] = df["published_date"].apply(normalize_date)
    
    # -----------------------------------------------------  
    # Extract date parts( year, month and quarter)
    # -----------------------------------------------------
    df = extract_date_parts(df)
    
    # -----------------------------------------------------
    # Normalize the "category" column
    # -----------------------------------------------------
    df["category"] = df["category"].apply(normalize_category)
    
    
    # -----------------------------------------------------
    # Company name validation 
    # -----------------------------------------------------
    df = validate_company_names(df, metadata)
    
    # -----------------------------------------------------
    # Enrichment (join news_data with company's metadata)
    # -----------------------------------------------------
    df = df.merge(metadata, on="company_name", how="left")

    # -----------------------------------------------------
    # Add derived field: `company_age`:
    # -----------------------------------------------------
    df["company_age"] = df.apply(
        lambda x: compute_company_age(x["founded_year"], x["published_date"]), axis=1
    )
    
    # -----------------------------------------------------
    # Add derived field: `company_size_category`: 
    # -----------------------------------------------------
    df["company_size_category"] = df["employee_count"].map(company_size_category)

    # -----------------------------------------------------
    # Embeddings
    # -----------------------------------------------------
    df = add_embeddings(df)

    # -----------------------------------------------------
    # Add similarity column
    # -----------------------------------------------------
    df = add_top_similar(df)



    # -----------------------------
    # Load into DuckDB  
    # DuckDB Integration with Vector Storage
    # -----------------------------
    load_to_duckdb(df)

    # -----------------------------------------------------
    # Filter function
    # -----------------------------------------------------
    df = filter_ai_articles(df)

   

    # -----------------------------------------------------
    # Export function to write output data in csv
    # -----------------------------------------------------
    export_csv(df)
    

    

    validation_report = run_data_validation(df)

    save_validation_report(validation_report)

    if any(validation_report["schema"]):
        raise Exception("Schema validation failed!")
    
    print("Pipeline completed successfully!")

if __name__ == '__main__':
    run_pipeline()
    