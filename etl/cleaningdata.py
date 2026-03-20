import re
import numpy as np
import pandas as pd

CURRENCY_RATES = {
    "EUR": 1.1,
    "GBP": 1.27,
    "JPY": 1/150,
    "USD": 1
}

def parse_revenue(value):
    if not value or str(value).lower() in ["nan", "n/a", "not disclosed"]:
        return 0

    value = str(value).replace(",", "").strip()

    # Handle range
    if "-" in value:
        parts = value.split("-")
        nums = [parse_revenue(p.strip()) for p in parts]
        return int(sum(nums) / len(nums))

    multiplier = 1
    if "B" in value or "billion" in value.lower():
        multiplier = 1e9
    elif "M" in value or "million" in value.lower():
        multiplier = 1e6

    num = float(re.findall(r"\d+\.?\d*", value)[0])

    # Detect currency
    currency = "USD"
    if "€" in value:
        currency = "EUR"
    elif "£" in value:
        currency = "GBP"
    elif "¥" in value:
        currency = "JPY"

    usd_value = num * multiplier * CURRENCY_RATES[currency]
    return int(usd_value)

def normalize_date(date_val):
    try:
        dt = pd.to_datetime(date_val, dayfirst=True, format='mixed', utc=True, errors='coerce')
        return dt
    except:
        return pd.NaT

def extract_date_parts(df):
    df = df.dropna(subset=['published_date'])
    df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce')
    df["year"] = df["published_date"].dt.year
    df["month"] = df["published_date"].dt.month
    df["quarter"] = df["published_date"].dt.to_period("Q")
    df['quarter'] = df['quarter'].astype(str)
    return df

# Category Maping
CATEGORY_MAP = {
    "ai/ml": "AI_ML",
    "artificial intelligence": "AI_ML",
    "machine learning": "AI_ML",
    "cloud": "CLOUD",
    "cloud computing": "CLOUD",
    "data analytics": "DATA_ANALYTICS"
}

def normalize_category(cat):
    if not cat:
        return "OTHER"
    cat = cat.lower().strip()
    return CATEGORY_MAP.get(cat, cat.upper())