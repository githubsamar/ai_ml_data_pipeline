import pandas as pd


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