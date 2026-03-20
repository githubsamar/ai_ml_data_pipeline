import polars as pl
import re
from datetime import datetime

# -----------------------------
# Revenue Cleaning
# -----------------------------
def clean_revenue(value):
    if value is None:
        return 0

    value = str(value).lower().replace(",", "").strip()

    if value in ["nan", "n/a", "not disclosed", "null"]:
        return 0

    # Handle ranges
    if "-" in value:
        parts = value.split("-")
        vals = [clean_revenue(p.strip()) for p in parts]
        return int(sum(vals) / len(vals))

    multiplier = 1

    if "€" in value:
        multiplier = 1.1
    elif "£" in value:
        multiplier = 1.27
    elif "¥" in value:
        multiplier = 1 / 150

    value = re.sub(r"[^\d\.a-z]", "", value)

    if "b" in value:
        num = float(re.findall(r"\d+\.?\d*", value)[0])
        return int(num * 1e9 * multiplier)

    if "m" in value:
        num = float(re.findall(r"\d+\.?\d*", value)[0])
        return int(num * 1e6 * multiplier)

    if "k" in value:
        num = float(re.findall(r"\d+\.?\d*", value)[0])
        return int(num * 1e3 * multiplier)

    try:
        return int(float(value) * multiplier)
    except:
        return 0


# -----------------------------
# Date Normalization
# -----------------------------
def normalize_date(value):
    if value is None:
        return None

    formats = ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except:
            return datetime.NaT

    try:
        return datetime.fromisoformat(value)
    except:
        return None


# -----------------------------
# Category Standardization
# -----------------------------
CATEGORY_MAP = {
    "ai/ml": "AI_ML",
    "artificial intelligence": "AI_ML",
    "machine learning": "AI_ML",
    "cloud": "CLOUD",
    "cloud computing": "CLOUD",
    "data analytics": "DATA",
}


def standardize_category(cat):
    if cat is None:
        return "OTHER"

    cat = str(cat).lower()

    for key in CATEGORY_MAP:
        if key in cat:
            return CATEGORY_MAP[key]

    return cat.upper()