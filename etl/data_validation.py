import json

REQUIRED_COLUMNS = {
    "article_id": "str",
    "title": "str",
    "company_name": "str",
    "published_date": "datetime64[us, UTC]",
    "revenue_usd": "int64"
}



def validate_schema(df):
    errors = []

    for col, dtype in REQUIRED_COLUMNS.items():
        if col not in df.columns:
            errors.append(f"Missing column: {col}")
        elif str(df[col].dtype) != dtype:
            errors.append(f"Column {col} has wrong type: {df[col].dtype}")

    return errors

def validate_revenue(df):
    issues = []

    if (df["revenue_usd"] < 0).any():
        issues.append("Negative revenue found")

    if (df["revenue_usd"] > 1e13).any():
        issues.append("Unrealistically large revenue detected")

    return issues


def validate_dates(df):
    issues = []

    if df["published_date"].isna().sum() > 0:
        issues.append("Missing/invalid dates present")

    return issues


def check_duplicates(df):
    dupes = df[df.duplicated(subset=["article_id"])]

    return len(dupes)

CRITICAL_COLUMNS = ["article_id", "title", "company_name"]

def check_nulls(df):
    issues = {}

    for col in CRITICAL_COLUMNS:
        null_count = df[col].isna().sum()
        if null_count > 0:
            issues[col] = null_count

    return issues

def run_data_validation(df):
    report = {}

    report["schema"] = validate_schema(df)
    report["revenue"] = validate_revenue(df)
    report["dates"] = validate_dates(df)
    report["duplicates"] = check_duplicates(df)
    report["nulls"] = check_nulls(df)

    return report

def save_validation_report(report):
    with open("./data/validation_report/data_quality_report.json", "w") as f:
        json.dump(report, f, indent=4, default=str)
        

