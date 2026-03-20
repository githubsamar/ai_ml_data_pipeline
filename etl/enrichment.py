import pandas as pd
from fuzzywuzzy import process

def match_company(name, metadata_names):
    match, score = process.extractOne(name, metadata_names)
    return match if score > 80 else None


def compute_company_age(founded_year, published_date):
    if pd.isna(founded_year) or pd.isna(published_date):
        return None
    return published_date.year - founded_year

def company_size_category(emp_count):
    if emp_count < 10000:
        return "Small"
    elif emp_count <= 30000:
        return "Medium"
    return "Large"